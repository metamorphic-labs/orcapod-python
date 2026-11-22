"""The golden ``Rejoin`` cases pin one set of values per diamond (ITL-1336).

``pipeline_rejoin_arrangement`` in ``cases.py`` pins ``Rejoin`` over each column role of
the tags (plain, data-tags, mixed), each in several arrangements of the tag order and the
table layout, on a plain stream and in a job on the sync and the async orchestrator.
Nothing may depend on the arrangement, and the async path must give the sync path's
hashes, rows and system-tag names, so those entries must be equal. ``pipeline_rejoin``
pins reordered inputs, which must give the first order's values (``Rejoin`` is
commutative), and ``pipeline_rejoin_job_async`` the job of ``pipeline_rejoin_job``.
"""

from __future__ import annotations

from typing import Any

from tests.test_hashing.golden.cases import (
    REJOIN_ARRANGEMENTS,
    REJOIN_ROLES,
    _rejoin_arranged,  # pyright: ignore[reportPrivateUsage]  # the cases' own input
    pipeline_rejoin,
    pipeline_rejoin_arrangement,
    pipeline_rejoin_job,
    pipeline_rejoin_job_async,
    stream_summary,
)

KINDS = ("stream", "job_sync", "job_async")


def _part(entries: dict[str, Any], prefix: str) -> dict[str, Any]:  # pyright: ignore[reportExplicitAny]  # golden entries
    """The entries under ``prefix``, with the prefix removed."""
    return {
        key.removeprefix(f"{prefix}."): value
        for key, value in entries.items()
        if key.startswith(f"{prefix}.")
    }


def test_every_arrangement_of_a_role_pins_the_same_values() -> None:
    entries = pipeline_rejoin_arrangement()
    for kind in KINDS:
        for role in REJOIN_ROLES:
            first, *others = (
                _part(entries, f"{kind}.{role}.{name}") for name in REJOIN_ARRANGEMENTS
            )
            assert first, (kind, role)
            assert others
            for other in others:
                assert other == first, (kind, role)


def test_the_async_job_pins_the_sync_job_and_the_stream() -> None:
    entries = pipeline_rejoin_arrangement()
    for role in REJOIN_ROLES:
        for name in REJOIN_ARRANGEMENTS:
            stream = _part(entries, f"stream.{role}.{name}")
            assert stream
            assert _part(entries, f"job_sync.{role}.{name}") == stream, (role, name)
            assert _part(entries, f"job_async.{role}.{name}") == stream, (role, name)


def test_each_role_keeps_its_bindings_and_the_inputs_system_tags() -> None:
    entries = pipeline_rejoin_arrangement()
    expected_data_tags = {
        "plain": [],
        "data_tags": ["cohort", "subject"],
        "mixed": ["subject"],
    }
    content_hashes = set[str]()
    for role, tag_only in REJOIN_ROLES.items():
        tags, layout = REJOIN_ARRANGEMENTS["tags_first"]
        origin = stream_summary(_rejoin_arranged(tags, layout, tag_only))
        pinned = _part(entries, f"stream.{role}.tags_first")
        assert pinned["data_tags"] == expected_data_tags[role]
        assert pinned["tag_keys"] == ["cohort", "subject"]
        assert pinned["system_tag_columns"] == origin["system_tag_columns"]
        content_hashes.add(pinned["content_hash"])
    assert len(content_hashes) == len(REJOIN_ROLES)


def test_reordered_inputs_and_the_method_pin_the_first_orders_values() -> None:
    entries = pipeline_rejoin()
    for first, other in (
        ("two", "two_reordered"),
        ("three", "three_reordered"),
        ("two", "method"),
    ):
        assert _part(entries, first)
        assert _part(entries, other) == _part(entries, first), other


def test_the_async_job_pins_the_sync_jobs_values() -> None:
    sync = pipeline_rejoin_job()
    assert sync
    assert pipeline_rejoin_job_async() == sync
