"""Retained row views have one stored representation and an explicit lifetime."""

from dataclasses import dataclass
from typing import Any, cast

from orcapod.core.datagrams import Data, Tag
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams.arrow_table_stream import ArrowTableStream


@dataclass
class StoredBase:
    value: int


@dataclass
class LiveChild(StoredBase):
    extra: int


def rows():
    """Live values whose stored declared class excludes the extra field."""
    return [
        (
            Tag({"k": 1}),
            Data(
                cast(Any, {"x": LiveChild(2, 9)}),
                python_schema={"x": StoredBase},
                meta_info={"note": "private"},
            ),
        )
    ]


def test_datagram_storage_view_preserves_original_and_hash() -> None:
    """Canonicalization belongs to the datagram and never mutates its live values."""
    original = rows()[0][1]
    stored = original.storage_view()
    assert type(stored.as_dict()["x"]) is StoredBase
    assert type(original.as_dict()["x"]) is LiveChild
    assert stored.storage_origin is original
    assert stored.content_hash() == original.content_hash()
    assert stored.meta_columns == original.meta_columns
    assert not original.storage_view(include_meta=False).meta_columns


def test_materialized_stream_rows_agree_with_stored_values() -> None:
    """Static materialization must not expose subclass-only Python fields."""
    stream = StaticOutputOperatorPod._materialize_to_stream(rows())
    [(_, data)] = list(stream.iter_data())
    assert type(data.as_dict()["x"]) is StoredBase
    assert not data.meta_columns
    decoded = Data(stream.as_table().select(["x"]), data_context=data.data_context_key)
    assert data.as_dict() == decoded.as_dict()


def test_arrow_row_factory_retains_canonical_objects() -> None:
    """The stream owns its row buffer through a factory that builds the same table."""
    stream = ArrowTableStream.from_rows(rows())
    first, second = list(stream.iter_data()), list(stream.iter_data())
    assert first[0][0] is second[0][0]
    assert first[0][1] is second[0][1]
    assert type(first[0][1].as_dict()["x"]) is StoredBase


def test_job_clear_cache_releases_rows_and_keeps_persistent_results() -> None:
    """A job can drop its in-memory working set and recover from its result store."""
    from tests.test_pipeline.test_execution_cache_reuse import make_chain

    job = make_chain()
    assert job.run().ok
    expected = job.nodes["total"].as_table(all_info=True)
    assert any(getattr(node, "_sync_row_views", {}) for node in job.nodes.values())
    job.clear_cache()
    for node in job.nodes.values():
        assert not getattr(node, "_sync_row_views", {})
        assert not getattr(node, "_entry_id_rows", {})
        assert getattr(node, "_completed_execution", None) is None
    assert job.run().ok
    assert job.nodes["total"].as_table(all_info=True).equals(expected)


def test_unsupported_source_emits_one_explanation_per_run(caplog) -> None:
    """Users can tell why an otherwise stable custom source misses the fast path."""
    import logging
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.sources import DictSource
    from tests.test_pipeline.test_execution_cache_reuse import doubled

    class CustomSource(DictSource):
        pass

    with PipelineJob(store=InMemoryArrowDatabase()) as job:
        doubled.pod(
            CustomSource([{"k": 1, "x": 2}], tag_columns=["k"], source_id="custom")
        )
    with caplog.at_level(logging.INFO, logger="orcapod.core.nodes._execution_cache"):
        assert job.run().ok
    messages = [
        record.message
        for record in caplog.records
        if "Completed-work reuse disabled" in record.message
    ]
    assert len(messages) == 1
    assert "CustomSource" in messages[0]


def test_row_factory_preserves_source_values_with_table_nullability() -> None:
    """Combined source fields normalize nullability without changing payload IDs."""
    original = Data({"x": 1}, source_info={"x": "token"})
    stream = ArrowTableStream.from_rows([(Tag({"k": 1}), original)])
    [(_, retained)] = list(stream.iter_data())
    row_table = retained.as_table(columns={"source": True})
    combined = stream.as_table(columns={"source": True}).select(row_table.column_names)
    assert combined.to_pylist() == row_table.to_pylist()
    assert not combined.schema.field("_source_x").nullable
    assert retained.content_hash() == original.content_hash()
    # Persisting retained rows and decoding the table use the same normalized schema.
    decoded = Data(combined, data_context=original.data_context_key)
    from_rows, _ = StaticOutputOperatorPod._rows_to_table([(Tag({"k": 1}), retained)])
    from_table, _ = StaticOutputOperatorPod._rows_to_table([(Tag({"k": 1}), decoded)])
    assert from_rows.equals(from_table, check_metadata=True)


def test_row_factory_preserves_nondefault_context_in_export() -> None:
    """Retained rows and context-inclusive exports identify the same decoder."""
    from dataclasses import replace
    from orcapod import contexts
    from orcapod.system_constants import constants

    context = replace(contexts.get_default_context(), context_key="test:retained:rows")
    pairs = [
        (Tag({"k": 1}, data_context=context), Data({"x": 2}, data_context=context))
    ]
    stream = ArrowTableStream.from_rows(pairs)
    assert stream.as_table(columns={"context": True})[
        constants.CONTEXT_KEY
    ].to_pylist() == [context.context_key]
    stream.clear_cache()
    assert list(stream.iter_data())[0][1].data_context_key == context.context_key


def test_row_factory_rejects_provenance_override() -> None:
    """A row-preserving factory cannot override the backing table independently."""
    import pytest

    pairs = [(Tag({"k": 1}), Data({"x": 2}, source_info={"x": "original"}))]
    with pytest.raises(ValueError, match="source_info"):
        ArrowTableStream.from_rows(pairs, source_info={"x": "replacement"})


def test_row_factory_rejects_context_override() -> None:
    """A single-context stream cannot silently relabel retained rows."""
    from dataclasses import replace
    from orcapod import contexts
    import pytest

    context = replace(contexts.get_default_context(), context_key="test:retained:other")
    with pytest.raises(ValueError, match="context"):
        ArrowTableStream.from_rows(rows(), data_context=context)


def test_row_factory_rejects_system_tag_relabeling() -> None:
    """Column relabeling belongs before constructing retained canonical rows."""
    import pytest

    with pytest.raises(ValueError, match="system_tag_columns"):
        ArrowTableStream.from_rows(rows(), system_tag_columns=["k"])


def test_row_factory_accepts_tagless_nondefault_stream_rows() -> None:
    """Empty tags still belong to the row's context when a stream is retained."""
    from dataclasses import replace
    import pyarrow as pa
    from orcapod import contexts

    context = replace(
        contexts.get_default_context(), context_key="test:retained:tagless"
    )
    source = ArrowTableStream(pa.table({"x": [1]}), data_context=context)
    stream = ArrowTableStream.from_rows(list(source.iter_data()))
    assert list(stream.iter_data())[0][0].data_context_key == context.context_key


def test_storage_view_preserves_lazily_assigned_uuid() -> None:
    """The view shares identity even when no caller has requested the UUID yet."""
    original = Data({"x": 1})
    view = original.storage_view()
    assert view.datagram_uuid == original.datagram_uuid


def test_row_factory_preserves_unregistered_subclass_context() -> None:
    """Canonicalizing a subclass must keep its context object, not resolve its key."""
    from dataclasses import replace
    from orcapod import contexts
    from orcapod.system_constants import constants

    class CustomTag(Tag):
        pass

    class CustomData(Data):
        pass

    context = replace(
        contexts.get_default_context(), context_key="test:retained:subclass"
    )
    for tag_type, data_type in [
        (CustomTag, Data),
        (Tag, CustomData),
        (CustomTag, CustomData),
    ]:
        stream = ArrowTableStream.from_rows(
            [
                (
                    tag_type({"k": 1}, data_context=context),
                    data_type({"x": 2}, data_context=context),
                )
            ]
        )
        assert stream.data_context is context
        assert stream.as_table(columns={"context": True})[
            constants.CONTEXT_KEY
        ].to_pylist() == [context.context_key]
        for clear in (False, True):
            if clear:
                stream.clear_cache()
            [(tag, data)] = list(stream.iter_data())
            assert tag.data_context_key == data.data_context_key == context.context_key
