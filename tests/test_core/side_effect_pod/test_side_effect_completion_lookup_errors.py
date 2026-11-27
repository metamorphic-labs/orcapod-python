"""A failed prior-completion lookup names the side-effect node and the row (ITL-919).

A side-effect pod that tracks completion looks up each row's invocation record before
delivering it. When that lookup cannot read the database, the ``OrcapodDatabaseReadError`` names the
pod and the row's tag, as a failed write of the same record does (CodeRabbit on #290).
"""

from __future__ import annotations

from typing import Any

import pytest

from orcapod.core import row_ids
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import OrcapodDatabaseReadError
from orcapod.pipeline.job import PipelineJob
from orcapod.side_effects import (
    SideEffectJobNode,
    SideEffectPod,
    SideEffectPodConfig,
)
from orcapod.sources import DictSource


class UnreadableCompletions:
    """A store whose record-by-id lookups (the completion check) cannot read."""

    def __init__(self, inner: Any) -> None:
        """Wrap ``inner``."""
        self._inner = inner

    def at(self, *path_components: str) -> UnreadableCompletions:
        """A view with the same failure."""
        return UnreadableCompletions(self._inner.at(*path_components))

    def get_record_by_id(self, record_path: tuple[str, ...], *args: Any, **kwargs: Any):
        """Fail as an unreadable stored table would."""
        raise OrcapodDatabaseReadError(
            "/store/" + "/".join(record_path), tuple(record_path)
        )

    def __getattr__(self, name: str) -> Any:
        """Delegate everything else."""
        return getattr(self._inner, name)


def test_completion_lookup_failure_names_the_node_and_row() -> None:
    """The lookup's ``OrcapodDatabaseReadError`` carries the pod's label and the row's tag."""
    delivered: list[int] = []

    def deliver(value: int, ctx: Any) -> None:
        delivered.append(value)

    pod = SideEffectPod(deliver, config=SideEffectPodConfig(track_completion=True))
    store = UnreadableCompletions(InMemoryArrowDatabase())
    job = PipelineJob(name="effects", database=store)  # pyright: ignore[reportArgumentType]  # failing wrapper of a real store
    with job:
        pod.process(
            DictSource(
                [{"id": 0, "value": 10}], tag_columns=["id"], tag_only_columns=True
            )
        )

    with pytest.raises(OrcapodDatabaseReadError) as info:
        job.run()
    assert info.value.tag == {"id": 0}
    assert info.value.node_label is not None
    assert delivered == []


def test_delivery_precedes_pulling_the_next_lazy_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delivered: list[int] = []

    def deliver(value: int, ctx: Any) -> None:
        delivered.append(value)

    source = DictSource(
        [{"id": 0, "value": 10}, {"id": 1, "value": 20}, {"id": 2, "value": 99}],
        tag_columns=["id"],
        tag_only_columns=True,
    )
    rows = list(source.iter_data())

    def read_lazily():
        yield rows[0]
        yield rows[2] if delivered else rows[1]

    monkeypatch.setattr(source, "iter_data", read_lazily)
    node = SideEffectJobNode(SideEffectPod(deliver), source)
    node.attach_databases(InMemoryArrowDatabase())

    assert [tag.as_dict()["id"] for tag, _ in node.execute(source).iter_data()] == [
        0,
        2,
    ]
    assert delivered == [10, 99]


def test_a_later_completion_failure_keeps_the_delivered_prefix_and_row_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delivered: list[int] = []

    def deliver(value: int, ctx: Any) -> None:
        delivered.append(value)

    source = DictSource(
        [{"id": 0, "value": 10}, {"id": 1, "value": 20}],
        tag_columns=["id"],
        tag_only_columns=True,
    )
    pod = SideEffectPod(deliver)
    tag, data = list(source.iter_data())[1]
    bad_id = row_ids.entry_id(
        pod.data_context.arrow_hasher, tag, data
    ).to_prefixed_digest()
    store = InMemoryArrowDatabase()
    point_read = store.get_record_by_id
    bulk_read = store.get_records_by_column

    def read_point(path, record_id, *args, **kwargs):
        if record_id == bad_id:
            raise OrcapodDatabaseReadError("synthetic table", path)
        return point_read(path, record_id, *args, **kwargs)

    def read_batch(path, column, values, *args, **kwargs):
        if bad_id in values:
            raise OrcapodDatabaseReadError("synthetic table", path)
        return bulk_read(path, column, values, *args, **kwargs)

    monkeypatch.setattr(store, "get_record_by_id", read_point)
    monkeypatch.setattr(store, "get_records_by_column", read_batch)
    node = SideEffectJobNode(pod, source)
    node.attach_databases(store)

    with pytest.raises(OrcapodDatabaseReadError) as info:
        node.execute(source)
    assert delivered == [10]
    assert info.value.tag == {"id": 1}
    assert info.value.node_label == pod.label
