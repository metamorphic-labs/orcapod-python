"""A loaded job gives an ordered operator its inputs in argument order (ITL-1417).

``MergeJoin`` and ``SemiJoin`` take their inputs in order. A saved job used to be rebuilt
with an operator's inputs in the order its edges were listed, which follows the order
the steps were recorded in: with another step over the second input recorded first, a
loaded ``MergeJoin(left, right)`` ran as ``MergeJoin(right, left)``. The saved file now
lists each operator's inputs in argument order (``inputs``). A file saved before that (a
legacy file) loads an operator over two or more inputs only when the operator is
positively known to be commutative; anything else is refused.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import subprocess
import sys
from collections.abc import Callable, Collection
from pathlib import Path
from typing import Any

import pytest

from orcapod import Pipeline, PipelineJob, operators, sources
from orcapod.core.operators import Index, Join, MergeJoin, Rejoin, SemiJoin
from orcapod.core.operators.base import BinaryOperator, NonZeroInputOperator
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline.serialization import OPERATOR_INPUTS_KEY, register_operator
from orcapod.types import CacheMode

ORDERED = {"MergeJoin", "SemiJoin"}
COMMUTATIVE = {"Join", "Rejoin"}
LEGACY_REFUSAL = "does not say in which order"


def test_every_multi_input_operator_is_classified() -> None:
    """The operators whose argument order matters are exactly ``ORDERED``; a new
    multi-input operator fails here until it is classified and has a case below."""
    ordered: set[str] = set()
    commutative: set[str] = set()
    for name in operators.__all__:
        cls = getattr(operators, name)
        if not inspect.isclass(cls) or not issubclass(
            cls, (BinaryOperator, NonZeroInputOperator)
        ):
            continue
        # Placeholders stand for two different streams: only the kind of the
        # returned group (tuple: ordered, frozenset: commutative) is read.
        two_inputs: Any = ("a", "b")
        symmetry = cls().argument_symmetry(two_inputs)
        (ordered if isinstance(symmetry, tuple) else commutative).add(name)
    assert ordered == ORDERED
    assert commutative == COMMUTATIVE


# ── inputs: two tags, two data columns (several-items rule) ──────────────────


def _csv(tmp_path: Path, name: str, header: str, rows: str) -> Any:
    path = tmp_path / f"{name}.csv"
    path.write_text(f"id,key,{header}\n{rows}")
    return sources.CSVSource(
        str(path), tag_columns=["id", "key"], tag_only_columns=True, source_id=name
    )


def _inputs(tmp_path: Path) -> dict[str, Any]:
    """``a`` and ``b`` share both data columns (``x``, ``y``); ``c`` has its own."""
    return {
        "a": _csv(tmp_path, "a", "x,y", "1,10,1,10\n2,20,2,20\n3,30,3,30\n"),
        "b": _csv(tmp_path, "b", "x,y", "1,10,100,1000\n2,20,101,1001\n"),
        "c": _csv(tmp_path, "c", "z,w", "1,10,42,43\n2,20,44,45\n"),
    }


# Each case builds ``step`` from its first and second argument; the order is visible
# in the rows (MergeJoin's lists, SemiJoin's kept side).
CASES: dict[str, Callable[[Any, Any, dict[str, Any]], Any]] = {
    "MergeJoin": lambda first, second, s: MergeJoin()(first, second, label="step"),
    "SemiJoin": lambda first, second, s: SemiJoin()(first, second, label="step"),
    "Join": lambda first, second, s: Join()(
        first.select_data_columns(["y"]), s["c"], label="step"
    ),
    "Rejoin": lambda first, second, s: Rejoin()(
        first.select_data_columns(["x"]),
        first.select_data_columns(["y"]),
        label="step",
    ),
    # Three inputs: a commutative Join of three, then an ordered MergeJoin over it.
    "Join3_then_MergeJoin": lambda first, second, s: MergeJoin()(
        Join()(
            first.select_data_columns(["x"]),
            s["c"],
            first.select_data_columns(["y"]),
            label="joined",
        ),
        second.select_data_columns(["x"]),
        label="step",
    ),
}


def _rows(table: Any) -> list[str]:
    return sorted(repr(sorted(row.items())) for row in table.to_pylist())


def _build(
    tmp_path: Path, case: str, other_use_first: bool, reverse: bool = False
) -> tuple[PipelineJob, Path]:
    """The job, with another step over the second argument recorded before or after
    the case, and the arguments in either order; ``step`` stores its rows."""
    streams = _inputs(tmp_path)
    first, second = (
        (streams["b"], streams["a"]) if reverse else (streams["a"], streams["b"])
    )
    db_path = tmp_path / "db"
    db = DeltaTableDatabase(db_path)
    job = PipelineJob(name="ordered", database=db)
    with job:
        if other_use_first:
            second.select_data_columns(["y"], label="other_use")
        CASES[case](first, second, streams)
        if not other_use_first:
            second.select_data_columns(["y"], label="other_use")
    job.nodes["step"].attach_databases(
        pipeline_database=db.at("ordered"), cache_mode=CacheMode.LOG
    )
    return job, db_path


def _run(job: PipelineJob, mode: str, **kwargs: Any) -> None:
    if mode == "sync":
        job.run(**kwargs)
    else:
        asyncio.run(job.run_async(**kwargs))


def _identity(node: Any) -> tuple[Any, ...]:
    return (node.content_hash(), node.pipeline_hash())


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("reverse", [False, True], ids=["a_b", "b_a"])
@pytest.mark.parametrize("other_use_first", [False, True])
@pytest.mark.parametrize("case", sorted(CASES))
def test_a_loaded_job_gives_the_rows_and_identity_of_the_live_job(
    case: str, other_use_first: bool, reverse: bool, mode: str, tmp_path: Path
) -> None:
    """Live rows (with system tags and source info) equal the rows of the saved and
    loaded job run with ``load_only=True``, and so do its content hash, pipeline hash
    and stored table (read through ``as_source``, which reads the stored path),
    whatever order the steps were recorded in and the arguments given in."""
    job, db_path = _build(tmp_path, case, other_use_first, reverse)
    _run(job, mode)
    live_rows = _rows(job.step.as_table(all_info=True))
    live_stored = _rows(job.step.as_source().as_table())
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(
        tmp_path / "job.json", database=DeltaTableDatabase(db_path)
    )
    _run(loaded, mode, load_only=True)
    assert _rows(loaded.step.as_table(all_info=True)) == live_rows
    assert _identity(loaded.step) == _identity(job.step)
    assert _rows(loaded.step.as_source().as_table()) == live_stored


@pytest.mark.parametrize("reverse", [False, True], ids=["a_b", "b_a"])
@pytest.mark.parametrize("other_use_first", [False, True])
@pytest.mark.parametrize("case", ["MergeJoin", "SemiJoin"])
def test_the_saved_file_lists_an_operators_inputs_in_argument_order(
    case: str, other_use_first: bool, reverse: bool, tmp_path: Path
) -> None:
    job, _ = _build(tmp_path, case, other_use_first, reverse)
    job.save(tmp_path / "job.json")
    nodes = json.loads((tmp_path / "job.json").read_text())["pipeline"]["nodes"]
    label_of = {key: node["label"] for key, node in nodes.items()}
    step = next(node for node in nodes.values() if node["label"] == "step")
    expected = ["b", "a"] if reverse else ["a", "b"]
    assert [label_of[key] for key in step[OPERATOR_INPUTS_KEY]] == expected


def test_a_loaded_file_saves_again_with_the_same_inputs(tmp_path: Path) -> None:
    """Saving a loaded job again writes the same ``inputs`` and edges."""
    job, db_path = _build(tmp_path, "MergeJoin", True)
    job.save(tmp_path / "first.json")
    loaded = PipelineJob.load(
        tmp_path / "first.json", database=DeltaTableDatabase(db_path)
    )
    loaded.save(tmp_path / "second.json")
    first = json.loads((tmp_path / "first.json").read_text())["pipeline"]
    second = json.loads((tmp_path / "second.json").read_text())["pipeline"]
    assert second["edges"] == first["edges"]
    assert {
        key: node.get(OPERATOR_INPUTS_KEY) for key, node in second["nodes"].items()
    } == {key: node.get(OPERATOR_INPUTS_KEY) for key, node in first["nodes"].items()}


# ── legacy files (saved before ITL-1417: main's saved form, no ``inputs``) ───


def _strip_inputs(path: Path, *, job_file: bool = True) -> None:
    """Make the file one saved before ITL-1417 (main's saved form): no ``inputs``."""
    data = json.loads(path.read_text())
    nodes = data["pipeline"]["nodes"] if job_file else data["nodes"]
    for node in nodes.values():
        node.pop(OPERATOR_INPUTS_KEY, None)
    path.write_text(json.dumps(data))


@pytest.mark.parametrize("other_use_first", [False, True])
@pytest.mark.parametrize("case", ["MergeJoin", "SemiJoin", "Join3_then_MergeJoin"])
def test_a_legacy_file_with_an_ordered_operator_is_refused(
    case: str, other_use_first: bool, tmp_path: Path
) -> None:
    job, db_path = _build(tmp_path, case, other_use_first)
    job.save(tmp_path / "job.json")
    _strip_inputs(tmp_path / "job.json")
    with pytest.raises(ValueError, match=LEGACY_REFUSAL):
        PipelineJob.load(tmp_path / "job.json", database=DeltaTableDatabase(db_path))


@pytest.mark.parametrize("other_use_first", [False, True])
@pytest.mark.parametrize("case", ["Join", "Rejoin"])
def test_a_legacy_file_with_commutative_operators_still_loads(
    case: str, other_use_first: bool, tmp_path: Path
) -> None:
    """A commutative operator does not depend on its input order: such a legacy file
    loads, as on main, and gives the live rows."""
    job, db_path = _build(tmp_path, case, other_use_first)
    job.run()
    live = _rows(job.step.as_table())
    job.save(tmp_path / "job.json")
    _strip_inputs(tmp_path / "job.json")
    loaded = PipelineJob.load(
        tmp_path / "job.json", database=DeltaTableDatabase(db_path)
    )
    loaded.run(load_only=True)
    assert _rows(loaded.step.as_table()) == live


class SchemaAwareJoin(Join):
    """A commutative custom operator that inspects its input streams, as the
    ``argument_symmetry`` contract allows (Codex review of PR 513, finding 1)."""

    def argument_symmetry(self, streams: Collection[Any]) -> Any:
        for stream in streams:
            stream.output_schema()
        return super().argument_symmetry(streams)


class SchemaAwareSemiJoin(SemiJoin):
    """An ordered custom operator that inspects its input streams (finding 1)."""

    def argument_symmetry(self, streams: Collection[Any]) -> Any:
        for stream in streams:
            stream.output_schema()
        return super().argument_symmetry(streams)


class NestedMergeJoin(MergeJoin):
    """An ordered operator stating its order as a nested group, which the
    ``argument_symmetry`` contract allows (finding 2)."""

    def argument_symmetry(self, streams: Collection[Any]) -> Any:
        return frozenset((tuple(streams),))


register_operator("SchemaAwareJoin", SchemaAwareJoin)
register_operator("SchemaAwareSemiJoin", SchemaAwareSemiJoin)
register_operator("NestedMergeJoin", NestedMergeJoin)


def _legacy_custom(
    tmp_path: Path, make: Callable[[dict[str, Any]], Any], blueprint: bool
) -> tuple[Path, Path, PipelineJob]:
    streams = _inputs(tmp_path)
    db_path = tmp_path / "db"
    job = PipelineJob(name="custom", database=DeltaTableDatabase(db_path))
    with job:
        streams["b"].select_data_columns(["y"], label="other_use")
        make(streams)
    job.run()
    path = tmp_path / ("pipeline.json" if blueprint else "job.json")
    if blueprint:
        job.pipeline.save(path)
    else:
        job.save(path)
    _strip_inputs(path, job_file=not blueprint)
    return path, db_path, job


@pytest.mark.parametrize("blueprint", [False, True], ids=["job", "blueprint"])
def test_a_legacy_custom_commutative_operator_that_reads_its_streams_loads(
    blueprint: bool, tmp_path: Path
) -> None:
    """Finding 1: the symmetry is asked on the rebuilt input streams, never on hash
    strings, so a commutative operator that inspects them loads as on main."""
    path, db_path, job = _legacy_custom(
        tmp_path,
        lambda s: SchemaAwareJoin()(
            s["a"].select_data_columns(["y"]), s["c"], label="step"
        ),
        blueprint,
    )
    if blueprint:
        assert "step" in Pipeline.load(path).nodes
    else:
        loaded = PipelineJob.load(path, database=DeltaTableDatabase(db_path))
        loaded.run(load_only=True)
        assert _rows(loaded.step.as_table()) == _rows(job.step.as_table())


@pytest.mark.parametrize("blueprint", [False, True], ids=["job", "blueprint"])
@pytest.mark.parametrize(
    "make",
    [
        pytest.param(
            lambda s: SchemaAwareSemiJoin()(s["a"], s["b"], label="step"),
            id="ordered_reads_streams",
        ),
        pytest.param(
            lambda s: NestedMergeJoin()(s["a"], s["b"], label="step"), id="nested"
        ),
    ],
)
def test_a_legacy_custom_operator_not_known_commutative_is_refused(
    make: Callable[[dict[str, Any]], Any], blueprint: bool, tmp_path: Path
) -> None:
    """Findings 1 and 2: an ordered operator that inspects its streams, or one stating
    its order as a nested group, is refused with the migration error (never an
    ``AttributeError``, never loaded with a guessed order)."""
    path, db_path, _ = _legacy_custom(tmp_path, make, blueprint)
    with pytest.raises(ValueError, match=LEGACY_REFUSAL):
        if blueprint:
            Pipeline.load(path)
        else:
            PipelineJob.load(path, database=DeltaTableDatabase(db_path))


_WRITER_ONLY = """from dataclasses import dataclass
from pathlib import Path
import sys
from orcapod import PipelineJob, function_pod, sources
from orcapod.core.operators import {operator}
from orcapod.databases import DeltaTableDatabase


@dataclass
class WriterOnly:
    value: int


@function_pod(output_keys=("payload", "x"))
def make_payload(x: int) -> tuple[WriterOnly, int]:
    return WriterOnly(x), x


root = Path(sys.argv[1])
(root / "a.csv").write_text("id,key,x\\n1,10,1\\n2,20,2\\n")
(root / "b.csv").write_text("id,key,y\\n1,10,100\\n2,20,101\\n")
a, b = (sources.CSVSource(str(root / f"{{n}}.csv"), tag_columns=["id", "key"],
        tag_only_columns=True, source_id=n) for n in ("a", "b"))
job = PipelineJob(name="unknown", database=DeltaTableDatabase(root / "store"))
with job:
    b.select_data_columns(["y"], label="other_use")
    {operator}()(make_payload.pod(a, label="producer"), b, label="step")
job.run()
job.save(root / "job.json")
job.pipeline.save(root / "pipeline.json")
"""


def _write_unregistered(tmp_path: Path, operator: str) -> None:
    script = tmp_path / "writer.py"
    script.write_text(_WRITER_ONLY.format(operator=operator))
    subprocess.run([sys.executable, str(script), str(tmp_path)], check=True)


@pytest.mark.parametrize("operator", ["MergeJoin", "Join"])
@pytest.mark.parametrize("blueprint", [False, True], ids=["job", "blueprint"])
def test_a_legacy_operator_kept_read_only_is_refused(
    operator: str, blueprint: bool, tmp_path: Path
) -> None:
    """Finding 3: an operator over a type this process has not registered is kept as
    a read-only stub, so nothing proves it commutative: a legacy file is refused (job
    file and standalone blueprint), even for a Join."""
    _write_unregistered(tmp_path, operator)
    path = tmp_path / ("pipeline.json" if blueprint else "job.json")
    _strip_inputs(path, job_file=not blueprint)
    with pytest.raises(ValueError, match=LEGACY_REFUSAL):
        if blueprint:
            Pipeline.load(path)
        else:
            PipelineJob.load(path)


@pytest.mark.parametrize("blueprint", [False, True], ids=["job", "blueprint"])
def test_a_current_file_with_an_operator_kept_read_only_loads(
    blueprint: bool, tmp_path: Path
) -> None:
    """The same files with ``inputs`` load: the order is known."""
    _write_unregistered(tmp_path, "MergeJoin")
    path = tmp_path / ("pipeline.json" if blueprint else "job.json")
    loaded = Pipeline.load(path) if blueprint else PipelineJob.load(path)
    assert "step" in loaded.nodes


@pytest.mark.parametrize("reverse", [False, True], ids=["a_b", "b_a"])
def test_a_loaded_read_only_node_keeps_its_inputs(
    reverse: bool, tmp_path: Path
) -> None:
    """Finding 4: a node kept read-only on load (``Index``, ITL-1326) still names its
    input, as on main; an ordered node names its inputs in argument order."""
    streams = _inputs(tmp_path)
    first, second = (
        (streams["b"], streams["a"]) if reverse else (streams["a"], streams["b"])
    )
    db_path = tmp_path / "db"
    job = PipelineJob(name="ro", database=DeltaTableDatabase(db_path))
    with job:
        merged = MergeJoin()(first, second, label="merged")
        Index("x", 0)(merged, label="indexed")
    job.run()
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(
        tmp_path / "job.json", database=DeltaTableDatabase(db_path)
    )
    assert [up.label for up in loaded.indexed.upstreams] == ["merged"]
    assert [up.label for up in loaded.merged.upstreams] == (
        ["b", "a"] if reverse else ["a", "b"]
    )


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("operator", [MergeJoin, SemiJoin])
def test_one_stream_given_twice_fails_to_run_in_a_job(
    operator: type, mode: str, tmp_path: Path
) -> None:
    """Finding 5 (pre-existing on main, pinned; ITL-1419): a job's
    graph has one edge for two argument slots of one stream, so ``MergeJoin()(a, a)``
    and ``SemiJoin()(a, a)`` fail when the job runs, though they run as plain
    streams. A fix makes this test fail; it is then changed to the fixed behaviour."""
    a = _inputs(tmp_path)["a"]
    assert operator()(a, a).as_table().num_rows == 3
    job = PipelineJob(name="repeat", database=DeltaTableDatabase(tmp_path / "db"))
    with job:
        operator()(a, a, label="repeat")
    with pytest.raises((ValueError, IndexError)):
        _run(job, mode)


@pytest.mark.parametrize("operator", [MergeJoin, SemiJoin])
def test_a_legacy_binary_operator_over_one_stream_is_refused(
    operator: type, tmp_path: Path
) -> None:
    """CodeRabbit on PR 513: a legacy file holds one edge for ``SemiJoin()(a, a)`` (an
    edge list cannot repeat an edge), so the operator would be rebuilt with one input
    where it takes two. Its inputs cannot be restored, so the load refuses."""
    a = _inputs(tmp_path)["a"]
    db_path = tmp_path / "db"
    job = PipelineJob(name="repeat", database=DeltaTableDatabase(db_path))
    with job:
        operator()(a, a, label="repeat")
    job.save(tmp_path / "job.json")
    _strip_inputs(tmp_path / "job.json")
    with pytest.raises(ValueError, match=LEGACY_REFUSAL):
        PipelineJob.load(tmp_path / "job.json", database=DeltaTableDatabase(db_path))
