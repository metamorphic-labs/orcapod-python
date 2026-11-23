"""A loaded job gives an ordered operator its inputs in argument order (ITL-1417).

``MergeJoin`` and ``SemiJoin`` take their inputs in order. A saved job used to be rebuilt
with an operator's inputs in the order its edges were listed, which follows the order
the steps were recorded in: with another step over the second input recorded first, a
loaded ``MergeJoin(left, right)`` ran as ``MergeJoin(right, left)``. The saved file now
lists each operator's inputs in argument order (``inputs``), and a file saved before
that is refused when it holds an ordered operator over two inputs.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from orcapod import PipelineJob, operators, sources
from orcapod.core.operators import Join, MergeJoin, Rejoin, SemiJoin
from orcapod.core.operators.base import BinaryOperator, NonZeroInputOperator
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline.serialization import OPERATOR_INPUTS_KEY

ORDERED = {"MergeJoin", "SemiJoin"}
COMMUTATIVE = {"Join", "Rejoin"}


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


def _csv(tmp_path: Path, name: str, rows: str) -> Any:
    path = tmp_path / f"{name}.csv"
    path.write_text(rows)
    return sources.CSVSource(
        str(path), tag_columns=["id"], tag_only_columns=True, source_id=name
    )


def _inputs(tmp_path: Path) -> dict[str, Any]:
    """Three CSV sources over one key; ``left`` and ``right`` share the data column
    ``x``, ``third`` has its own."""
    return {
        "left": _csv(tmp_path, "left", "id,x,y\n1,1,10\n2,2,20\n3,3,30\n"),
        "right": _csv(tmp_path, "right", "id,x,z\n1,100,0\n2,101,0\n"),
        "third": _csv(tmp_path, "third", "id,w\n1,7\n2,8\n"),
    }


# Each case: the operator's name and the step over the inputs. ``left`` and ``right``
# are the two inputs of a binary operator, in argument order; the order is visible in
# the rows (MergeJoin's lists, SemiJoin's kept side).
CASES: dict[str, Callable[[dict[str, Any]], Any]] = {
    "MergeJoin": lambda s: MergeJoin()(s["left"], s["right"], label="step"),
    "SemiJoin": lambda s: SemiJoin()(s["left"], s["right"], label="step"),
    "Join": lambda s: Join()(
        s["left"].select_data_columns(["y"]), s["third"], label="step"
    ),
    "Rejoin": lambda s: Rejoin()(
        s["left"].select_data_columns(["x"]),
        s["left"].select_data_columns(["y"]),
        label="step",
    ),
    # Three inputs: a commutative Join of three, then an ordered MergeJoin over it.
    "Join3_then_MergeJoin": lambda s: MergeJoin()(
        Join()(
            s["left"].select_data_columns(["x"]),
            s["third"],
            s["left"].select_data_columns(["y"]),
            label="joined",
        ),
        s["right"].select_data_columns(["x"]),
        label="step",
    ),
}


def _rows(table: Any) -> list[str]:
    return sorted(repr(sorted(row.items())) for row in table.to_pylist())


def _build(
    tmp_path: Path, case: str, other_use_first: bool
) -> tuple[PipelineJob, Path]:
    """The job, with another step over ``right`` recorded before or after the case."""
    streams = _inputs(tmp_path)
    db_path = tmp_path / "db"
    job = PipelineJob(name="ordered", database=DeltaTableDatabase(db_path))
    with job:
        if other_use_first:
            streams["right"].select_data_columns(["z"], label="other_use")
        CASES[case](streams)
        if not other_use_first:
            streams["right"].select_data_columns(["z"], label="other_use")
    return job, db_path


def _run(job: PipelineJob, mode: str, **kwargs: Any) -> None:
    if mode == "sync":
        job.run(**kwargs)
    else:
        asyncio.run(job.run_async(**kwargs))


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("other_use_first", [False, True])
@pytest.mark.parametrize("case", sorted(CASES))
def test_a_loaded_job_gives_the_rows_of_the_live_job(
    case: str, other_use_first: bool, mode: str, tmp_path: Path
) -> None:
    """Live rows equal the rows of the saved and loaded job run with
    ``load_only=True``, whatever order the steps were recorded in."""
    job, db_path = _build(tmp_path, case, other_use_first)
    _run(job, mode)
    live = _rows(job.step.as_table())
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(
        tmp_path / "job.json", database=DeltaTableDatabase(db_path)
    )
    _run(loaded, mode, load_only=True)
    assert _rows(loaded.step.as_table()) == live


@pytest.mark.parametrize("other_use_first", [False, True])
@pytest.mark.parametrize("case", ["MergeJoin", "SemiJoin"])
def test_the_saved_file_lists_an_operators_inputs_in_argument_order(
    case: str, other_use_first: bool, tmp_path: Path
) -> None:
    job, _ = _build(tmp_path, case, other_use_first)
    job.save(tmp_path / "job.json")
    nodes = json.loads((tmp_path / "job.json").read_text())["pipeline"]["nodes"]
    label_of = {key: node["label"] for key, node in nodes.items()}
    step = next(node for node in nodes.values() if node["label"] == "step")
    assert [label_of[key] for key in step[OPERATOR_INPUTS_KEY]] == ["left", "right"]


def _strip_inputs(path: Path) -> None:
    """Make the file one saved before ITL-1417: no ``inputs`` on any operator."""
    data = json.loads(path.read_text())
    for node in data["pipeline"]["nodes"].values():
        node.pop(OPERATOR_INPUTS_KEY, None)
    path.write_text(json.dumps(data))


@pytest.mark.parametrize("other_use_first", [False, True])
@pytest.mark.parametrize("case", ["MergeJoin", "SemiJoin", "Join3_then_MergeJoin"])
def test_a_file_saved_before_the_fix_with_an_ordered_operator_is_refused(
    case: str, other_use_first: bool, tmp_path: Path
) -> None:
    """An ordered operator over two inputs in a file without ``inputs``: the order
    cannot be known, so the load refuses rather than guess."""
    job, db_path = _build(tmp_path, case, other_use_first)
    job.save(tmp_path / "job.json")
    _strip_inputs(tmp_path / "job.json")
    with pytest.raises(ValueError, match="does not say which comes first"):
        PipelineJob.load(tmp_path / "job.json", database=DeltaTableDatabase(db_path))


@pytest.mark.parametrize("other_use_first", [False, True])
@pytest.mark.parametrize("case", ["Join", "Rejoin"])
def test_a_file_saved_before_the_fix_with_commutative_operators_still_loads(
    case: str, other_use_first: bool, tmp_path: Path
) -> None:
    """A commutative operator does not depend on its input order: such an old file
    loads and gives the live rows."""
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
