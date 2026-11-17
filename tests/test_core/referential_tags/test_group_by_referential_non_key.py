"""``GroupBy`` over referential tags after ITL-1070 (ITL-1023, ITL-1017).

``GroupBy`` keeps only its ``by`` tags (ITL-1070). A referential tag is one column in two
roles (developer, November 13, 2026): a referential tag that is not in ``by`` loses its tag
role only. Its data column stays and is aggregated like every other data column, a list
with one element per member and one source-info token per member. A plain (tag-only)
non-key tag is dropped entirely. A referential ``by`` tag stays scalar in both roles and
keeps its one merged source-info token (pinned from a run at ``c78ba80a``).

The members of a group are ordered by the non-key tags, then by record id, as on main; so
the list of a referential non-key is ordered by its own values, and every other data list
follows that order. Both the sync path, ``op(stream)``, and the async path,
``async_execute`` over channels, are checked.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import orcapod as op
from orcapod.channels import Channel
from orcapod.core.operators import GroupBy
from orcapod.core.referential_tags import stream_references
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol

ROWS = [
    {"subject": "m1", "session": 2, "v": 0.7},
    {"subject": "m1", "session": 1, "v": 0.5},
    {"subject": "m2", "session": 1, "v": 0.9},
]

# Pinned from a run of this input at c78ba80a, before ITL-1070 was merged, and
# re-pinned when the token became the ``group_v1`` fold of the members' tokens in
# member order (ITL-1154 refresh): the folded token of the referential key must not
# move. ``m2`` has one member, so its digest is the one pinned before.
SUBJECT_TOKENS = {
    "m1": "group_v1:dc09a0e68d7b52eac555beced397606aea786ac869a0af46465a376874bb07c7",
    "m2": "group_v1:ec856d114f3bb89b2f626c9b1bc2a195e133da8c9b828d54f99a77dfa4332b40",
}
# Members ordered by ``session``: row_1 (session 1) before row_0 (session 2).
SESSION = {"m1": [1, 2], "m2": [1]}
SESSION_TOKENS = {
    "m1": ["sessions::row_1::session", "sessions::row_0::session"],
    "m2": ["sessions::row_2::session"],
}
V = {"m1": [0.5, 0.7], "m2": [0.9]}
V_TOKENS = {
    "m1": ["sessions::row_1::v", "sessions::row_0::v"],
    "m2": ["sessions::row_2::v"],
}


def _source(**kw: Any) -> StreamProtocol:
    """Tags ``subject`` and ``session``, both referential unless ``kw`` says otherwise."""
    return op.sources.DictSource(
        ROWS, tag_columns=["subject", "session"], source_id="sessions", **kw
    )


async def _run_async(
    group_by: GroupBy, stream: StreamProtocol
) -> list[tuple[TagProtocol, DataProtocol]]:
    channel: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=16)
    output: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=16)
    for row in stream.iter_data():
        await channel.writer.send(row)
    await channel.writer.close()
    await group_by.async_execute(
        [channel.reader], output.writer, input_pipeline_hashes=[stream.pipeline_hash()]
    )
    return await output.reader.collect()


def _sync_rows(**kw: Any) -> list[tuple[TagProtocol, DataProtocol]]:
    return list(GroupBy(["subject"])(_source(**kw)).iter_data())


def _async_rows(**kw: Any) -> list[tuple[TagProtocol, DataProtocol]]:
    return asyncio.run(_run_async(GroupBy(["subject"]), _source(**kw)))


def _check_rows(rows: Sequence[tuple[TagProtocol, DataProtocol]]) -> None:
    """Each row: only the key as tag; the key, ``session`` and ``v`` as data."""
    assert len(rows) == 2
    for tag, data in rows:
        subject = tag["subject"]
        assert isinstance(subject, str)
        assert tuple(tag.keys()) == ("subject",)
        assert set(data.keys()) == {"subject", "session", "v"}
        assert data["subject"] == subject
        assert data["session"] == SESSION[subject]
        assert data["v"] == V[subject]
        assert data.source_info() == {
            "subject": SUBJECT_TOKENS[subject],
            "session": SESSION_TOKENS[subject],
            "v": V_TOKENS[subject],
        }


def _check_plain_rows(rows: Sequence[tuple[TagProtocol, DataProtocol]]) -> None:
    """A plain non-key ``session`` is gone; it still orders the members."""
    assert len(rows) == 2
    for tag, data in rows:
        subject = tag["subject"]
        assert isinstance(subject, str)
        assert tuple(tag.keys()) == ("subject",)
        assert set(data.keys()) == {"subject", "v"}
        assert data["v"] == V[subject]
        assert "session" not in data.source_info()


def test_schema_prediction_matches_the_result() -> None:
    source = _source()
    group_by = GroupBy(["subject"])
    # Schema prediction checked against static_process (CLAUDE.md standing rule).
    assert (
        group_by.output_schema(source)
        == group_by.static_process(source).output_schema()
    )
    tag_schema, data_schema = group_by.output_schema(source)
    assert dict(tag_schema) == {"subject": str}
    assert dict(data_schema) == {"subject": str, "session": list[int], "v": list[float]}


def test_schema_prediction_matches_the_result_for_a_plain_non_key() -> None:
    source = _source(tag_only_columns=["session"])
    group_by = GroupBy(["subject"])
    # Schema prediction checked against static_process (CLAUDE.md standing rule).
    assert (
        group_by.output_schema(source)
        == group_by.static_process(source).output_schema()
    )
    tag_schema, data_schema = group_by.output_schema(source)
    assert dict(tag_schema) == {"subject": str}
    assert dict(data_schema) == {"subject": str, "v": list[float]}


def test_referential_non_key_loses_its_tag_role_only() -> None:
    grouped = GroupBy(["subject"])(_source())
    tags, data = grouped.keys()
    assert tags == ("subject",)
    assert set(data) == {"subject", "session", "v"}
    assert stream_references(grouped) == ("subject",)
    names = grouped.as_table(all_info=True).column_names
    assert names.count("subject") == 1
    assert names.count("session") == 1
    assert "_source_session" in names


def test_plain_non_key_is_dropped_entirely() -> None:
    grouped = GroupBy(["subject"])(_source(tag_only_columns=["session"]))
    tags, data = grouped.keys()
    assert tags == ("subject",)
    assert set(data) == {"subject", "v"}
    assert stream_references(grouped) == ("subject",)
    names = grouped.as_table(all_info=True).column_names
    assert "session" not in names and "_source_session" not in names


def test_sync_rows() -> None:
    _check_rows(_sync_rows())


def test_async_rows() -> None:
    _check_rows(_async_rows())


def test_sync_rows_plain_non_key() -> None:
    _check_plain_rows(_sync_rows(tag_only_columns=["session"]))


def test_async_rows_plain_non_key() -> None:
    _check_plain_rows(_async_rows(tag_only_columns=["session"]))


def test_sync_table_keeps_the_pinned_key_token() -> None:
    table = GroupBy(["subject"])(_source()).as_table(columns={"source": True})
    assert table.column("subject").to_pylist() == ["m1", "m2"]
    assert table.column("_source_subject").to_pylist() == [
        SUBJECT_TOKENS["m1"],
        SUBJECT_TOKENS["m2"],
    ]
    assert table.column("session").to_pylist() == [SESSION["m1"], SESSION["m2"]]
    assert table.column("_source_session").to_pylist() == [
        SESSION_TOKENS["m1"],
        SESSION_TOKENS["m2"],
    ]
    assert table.column("v").to_pylist() == [V["m1"], V["m2"]]
    assert table.column("_source_v").to_pylist() == [V_TOKENS["m1"], V_TOKENS["m2"]]


# An EDI-shaped series: the rows of trial 1 are not in time order, so the record-id
# order and the time order differ.
SERIES = [
    {"trial": 1, "timestamp": 30, "value": "c"},
    {"trial": 1, "timestamp": 10, "value": "a"},
    {"trial": 2, "timestamp": 5, "value": "x"},
    {"trial": 1, "timestamp": 20, "value": "b"},
]


def _series() -> StreamProtocol:
    return op.sources.DictSource(
        SERIES, tag_columns=["trial", "timestamp"], source_id="series"
    )


def _check_time_order(rows: Sequence[tuple[TagProtocol, DataProtocol]]) -> None:
    by_trial = {tag["trial"]: data for tag, data in rows}
    assert by_trial[1]["timestamp"] == [10, 20, 30]
    assert by_trial[1]["value"] == ["a", "b", "c"]
    assert by_trial[1].source_info()["timestamp"] == [
        "series::row_1::timestamp",
        "series::row_3::timestamp",
        "series::row_0::timestamp",
    ]
    assert by_trial[2]["timestamp"] == [5]
    assert by_trial[2]["value"] == ["x"]


def test_members_are_ordered_by_the_referential_non_key_sync() -> None:
    _check_time_order(list(GroupBy(["trial"])(_series()).iter_data()))


def test_members_are_ordered_by_the_referential_non_key_async() -> None:
    _check_time_order(asyncio.run(_run_async(GroupBy(["trial"]), _series())))
