"""A structural parameter receives its own class, in memory and from a store (ITL-972).

The case measured on November 12, 2026: two classes of one identity, the second with a
method the first lacks. Pod A returns the first; pod B is annotated with the second and
calls the method. In memory B used to receive the first class's object unchanged (the
method was missing), while through a Delta-backed job it received the second class. With
``orcapod.dataclass`` both paths must hand B an instance of the class it is annotated with.
"""

from __future__ import annotations

from pathlib import Path

import orcapod as op
from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource


@op.dataclass(name="itl972.measured.Q")
class QOld:
    """The first definition: no ``describe``."""

    x: int
    y: int


@op.dataclass(name="itl972.measured.Q")
class QNew:
    """The second definition: same name and fields, plus ``describe``."""

    x: int
    y: int

    def describe(self) -> str:
        """Return a text only this class can produce."""
        return f"QNew({self.x}, {self.y})"


@function_pod(output_keys="q")
def make(x: int) -> QOld:
    """Return a ``QOld``."""
    return QOld(x, x + 1)


@function_pod(output_keys="seen")
def look(q: QNew) -> str:
    """Return the class name of the argument and what its ``describe`` gives."""
    return f"{type(q).__name__}:{q.describe()}"


def _source() -> DictSource:
    """Return a two-row source of ``x`` values."""
    return DictSource(
        data=[{"k": "a", "x": 1}, {"k": "b", "x": 2}],
        tag_columns=["k"],
        source_id="s",
    )


EXPECTED = ["QNew:QNew(1, 2)", "QNew:QNew(2, 3)"]


def test_in_memory_chain_hands_the_annotated_class() -> None:
    """In memory, pod B receives ``QNew`` although pod A returned ``QOld``."""
    result = look.pod(make.pod(_source()))
    seen = sorted(data.as_dict()["seen"] for _, data in result.iter_data())
    assert seen == EXPECTED


def test_stored_chain_hands_the_annotated_class(tmp_path: Path) -> None:
    """Through a Delta-backed job, pod B receives ``QNew`` as well."""
    job = PipelineJob(name="measured", store=DeltaTableDatabase(tmp_path / "db"))
    with job:
        _ = look.pod(make.pod(_source(), label="make"), label="look")
    job.run()
    seen = sorted(data.as_dict()["seen"] for _, data in job.nodes["look"].iter_data())
    assert seen == EXPECTED


def test_stored_results_are_reused_and_still_hand_the_annotated_class(
    tmp_path: Path,
) -> None:
    """A second run reads pod A's stored rows; pod B still receives ``QNew``."""
    for _ in range(2):
        job = PipelineJob(name="measured", store=DeltaTableDatabase(tmp_path / "db"))
        with job:
            _ = look.pod(make.pod(_source(), label="make"), label="look")
        job.run()
        seen = sorted(
            data.as_dict()["seen"] for _, data in job.nodes["look"].iter_data()
        )
        assert seen == EXPECTED
