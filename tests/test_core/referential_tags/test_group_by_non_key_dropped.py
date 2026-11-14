"""``GroupBy`` over referential tags after ITL-1070 (ITL-1023, ITL-1017).

``GroupBy`` keeps only its ``by`` tags (ITL-1070). A referential tag is one column in
both roles, so a referential tag that is not in ``by`` leaves the output entirely: as a
tag, as a data column and with its source-info column. A referential ``by`` tag stays
scalar in both roles and keeps its one merged source-info token, exactly as before the
merge of ITL-1070 into the slice (pinned from a run at ``c78ba80a``). Both the sync path,
``op(stream)``, and the async path, ``async_execute`` over channels, are checked.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

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

# Pinned from a run of this input at c78ba80a, before ITL-1070 was merged: the folded
# token of the referential key must not move.
SUBJECT_TOKENS = {
    "m1": "merged:source_fold_v1:"
    "0e4f6dafc12b16f4daf438baa263f3a2bcefba8c5248dbccc3bd0dfd9c242b13",
    "m2": "merged:source_fold_v1:"
    "ec856d114f3bb89b2f626c9b1bc2a195e133da8c9b828d54f99a77dfa4332b40",
}
V = {"m1": [0.5, 0.7], "m2": [0.9]}
V_TOKENS = {
    "m1": ["sessions::row_1::v", "sessions::row_0::v"],
    "m2": ["sessions::row_2::v"],
}


def _source() -> StreamProtocol:
    """Tags ``subject`` and ``session``, both referential (the source default)."""
    source = op.sources.DictSource(
        ROWS, tag_columns=["subject", "session"], source_id="sessions"
    )
    assert stream_references(source) == ("subject", "session")
    return source


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


def _sync_rows() -> list[tuple[TagProtocol, DataProtocol]]:
    return list(GroupBy(["subject"])(_source()).iter_data())


def _async_rows() -> list[tuple[TagProtocol, DataProtocol]]:
    return asyncio.run(_run_async(GroupBy(["subject"]), _source()))


def _check_rows(rows: Sequence[tuple[TagProtocol, DataProtocol]]) -> None:
    """Each row: only the key as tag, the key and ``v`` as data, pinned source info."""
    assert len(rows) == 2
    for tag, data in rows:
        subject = tag["subject"]
        assert isinstance(subject, str)
        assert tuple(tag.keys()) == ("subject",)
        assert tuple(data.keys()) == ("subject", "v")
        assert data["subject"] == subject
        assert data["v"] == V[subject]
        source_names = data.as_table(columns={"source": True}).column_names
        assert "_source_session" not in source_names
        assert data.source_info() == {
            "subject": SUBJECT_TOKENS[subject],
            "v": V_TOKENS[subject],
        }


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
    assert dict(data_schema) == {"subject": str, "v": list[float]}


def test_referential_non_key_is_dropped_in_both_roles() -> None:
    grouped = GroupBy(["subject"])(_source())
    assert grouped.keys() == (("subject",), ("subject", "v"))
    assert stream_references(grouped) == ("subject",)
    names = grouped.as_table(all_info=True).column_names
    assert "session" not in names
    assert "_source_session" not in names
    assert names.count("subject") == 1


def test_sync_rows_keep_the_pinned_key_token() -> None:
    _check_rows(_sync_rows())


def test_async_rows_keep_the_pinned_key_token() -> None:
    _check_rows(_async_rows())


def test_sync_table_keeps_the_pinned_key_token() -> None:
    table = GroupBy(["subject"])(_source()).as_table(columns={"source": True})
    assert table.column("subject").to_pylist() == ["m1", "m2"]
    assert table.column("_source_subject").to_pylist() == [
        SUBJECT_TOKENS["m1"],
        SUBJECT_TOKENS["m2"],
    ]
    assert table.column("v").to_pylist() == [V["m1"], V["m2"]]
    assert table.column("_source_v").to_pylist() == [V_TOKENS["m1"], V_TOKENS["m2"]]
