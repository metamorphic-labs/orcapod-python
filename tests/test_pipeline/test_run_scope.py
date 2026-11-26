"""Every run that can write holds one run scope per database (ITL-1502, N44)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any, cast

from orcapod import function_pod
from orcapod.core.nodes import FunctionJobNode, OperatorJobNode
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import SelectDataColumns
from orcapod.sources import DictSource
from orcapod.types import CacheMode


class _Counted(InMemoryArrowDatabase):
    """An in-memory database that counts the run scopes entered."""

    def __init__(self) -> None:
        super().__init__()
        self.scopes = 0

    @contextmanager
    def _scope(self) -> Iterator[None]:
        self.scopes += 1
        yield

    def run_scope(self) -> AbstractContextManager[None]:
        return self._scope()


def _source() -> DictSource:
    return DictSource(
        data=[{"k": "a", "x": 1}, {"k": "b", "x": 2}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
    )


@function_pod(output_keys="y")
def _double(x: int) -> int:
    return 2 * x


def test_a_standalone_function_job_node_run_enters_the_run_scope() -> None:
    database = _Counted()
    node = FunctionJobNode(
        cast(Any, _double.pod), _source(), pipeline_database=database
    )
    node.run()
    assert database.scopes >= 1


def test_a_standalone_log_operator_job_node_run_enters_the_run_scope() -> None:
    database = _Counted()
    node = OperatorJobNode(
        SelectDataColumns(["x"]),
        [_source()],
        pipeline_database=database,
        cache_mode=CacheMode.LOG,
    )
    _ = node.run()
    assert database.scopes >= 1
