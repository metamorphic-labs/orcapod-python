"""The order of a group's members (ITL-1226).

Developer decision, November 17, 2026: ``GroupBy`` never depends on how its input's
columns are arranged. Without ``member_order``, the members of a group are ordered by the
non-key tags in name order, then by the record id and source id columns. With
``member_order``, by the tags it names, in that order, then by the other non-key tags in
name order, then by the same tiebreaks. The option is part of the operator's identity only
when given. No member is ever ordered by a data value.

For one set of rows, every arrangement of the input's tag and data columns (the order of
``tag_columns``, the physical order of the table, plain tags, data-tags and both, a source
and a raw stream) gives the same grouped rows: groups, member order in every list, the
folded record id and source id, the source-info tokens, the row hashes, and the stream's
content and pipeline hash. Only the output's column order follows the input: it is
presentation, and no hash depends on it.
"""

from __future__ import annotations

import itertools
import json
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.operators import GroupBy
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.pipeline.serialization import resolve_operator_from_config
from orcapod.core.streams.base import StreamBase
from orcapod.protocols.core_protocols import StreamProtocol

# ``x`` and ``y`` order the members differently; ``y`` and ``b`` hold nulls; rows 1 and
# 2 share every tag, so only the record id tells them apart.
COLUMNS: dict[str, pa.Array[Any]] = {
    "g": pa.array([1, 1, 1, 1, 1, 1], pa.int64()),
    "k": pa.array([1, 1, 1, 2, 2, 2], pa.int64()),
    "x": pa.array([2, 1, 1, 1, 2, 1], pa.int64()),
    "y": pa.array([1, 2, 2, None, 1, 3], pa.int64()),
    "a": pa.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6], pa.float64()),
    "b": pa.array(["p", "q", "r", "s", None, "u"], pa.large_string()),
}
TAGS = ("g", "k", "x", "y")
NOT_NULL = ("g", "k", "x")

# A few physical layouts: tags first, data first, interleaved, reversed.
LAYOUTS: tuple[tuple[str, ...], ...] = (
    ("g", "k", "x", "y", "a", "b"),
    ("a", "b", "y", "x", "k", "g"),
    ("x", "a", "g", "b", "y", "k"),
    ("b", "y", "a", "k", "x", "g"),
)
TAG_ORDERS = tuple(itertools.permutations(TAGS))

MODES = ("plain", "data_tags", "mixed")


def _table(layout: tuple[str, ...]) -> pa.Table:
    table = pa.table({name: COLUMNS[name] for name in layout})
    return table.cast(
        pa.schema(f.with_nullable(f.name not in NOT_NULL) for f in table.schema)
    )


def _input(
    mode: str, kind: str, tag_order: tuple[str, ...], layout: tuple[str, ...]
) -> StreamBase:
    table = _table(layout)
    if kind == "raw":
        data_tags = {
            "plain": [],
            "data_tags": list(tag_order),
            "mixed": [t for t in tag_order if t != "y"],
        }[mode]
        return ArrowTableStream(table, tag_columns=list(tag_order), data_tags=data_tags)
    tag_only: bool | list[str] = {"plain": True, "data_tags": False, "mixed": ["y"]}[
        mode
    ]
    return op.sources.ArrowTableSource(
        table, tag_columns=list(tag_order), tag_only_columns=tag_only, source_id="s"
    )


def _fingerprint(stream: StreamProtocol, *, content_hash: bool) -> dict[str, Any]:
    """Everything about a stream's rows and identity except its column order."""
    rows = sorted(
        json.dumps(row, sort_keys=True, default=str)
        for row in stream.as_table(all_info=True).to_pylist()
    )
    row_hashes = sorted(
        (tag.content_hash().to_string(), data.content_hash().to_string())
        for tag, data in stream.iter_data()
    )
    out: dict[str, Any] = {
        "rows": rows,
        "row_hashes": row_hashes,
        "pipeline_hash": stream.pipeline_hash().to_string(),
    }
    if content_hash:
        out["content_hash"] = stream.content_hash().to_string()
    return out


def _group_twice(
    stream: StreamBase, member_order: list[str] | None
) -> tuple[StreamBase, StreamBase]:
    once = stream.group_by(["g", "k"], member_order=member_order)
    # The grouped stream's only non-key tag is ``k``.
    twice = once.group_by(["g"], member_order=["k"] if member_order else None)
    return once, twice


# ── no dependence on the arrangement ────────────────────────────────────────


@pytest.mark.parametrize("member_order", [None, ["y"], ["y", "x"]])
@pytest.mark.parametrize("kind", ["source", "raw"])
@pytest.mark.parametrize("mode", MODES)
def test_no_arrangement_of_the_input_changes_the_result(
    mode: str, kind: str, member_order: list[str] | None
) -> None:
    # A raw stream's own content hash depends on its tag order (ITL-1227): only its
    # values and pipeline hash are compared.
    content_hash = kind == "source"
    seen: dict[str, Any] = {}
    for tag_order, layout in itertools.product(TAG_ORDERS, LAYOUTS):
        once, twice = _group_twice(_input(mode, kind, tag_order, layout), member_order)
        found = {
            "once": _fingerprint(once, content_hash=content_hash),
            "twice": _fingerprint(twice, content_hash=content_hash),
        }
        if not seen:
            seen = found
        assert found == seen, (tag_order, layout)


@pytest.mark.parametrize("member_order", [None, ["y"]])
@pytest.mark.parametrize("mode", MODES)
def test_a_job_gives_the_same_rows_sync_and_async_for_any_arrangement(
    mode: str, member_order: list[str] | None
) -> None:
    seen: list[str] = []
    for tag_order, layout in [
        (TAGS, LAYOUTS[0]),
        (("y", "x", "k", "g"), LAYOUTS[1]),
        (("x", "g", "y", "k"), LAYOUTS[2]),
    ]:
        for orchestrator in (SyncPipelineOrchestrator, AsyncPipelineOrchestrator):
            job = PipelineJob(name="order", database=InMemoryArrowDatabase())
            with job:
                _ = _input(mode, "source", tag_order, layout).group_by(
                    ["g", "k"], member_order=member_order, label="grouped"
                )
            job.run(orchestrator=orchestrator())
            rows = sorted(
                json.dumps(row, sort_keys=True, default=str)
                for row in job.nodes["grouped"].as_table(all_info=True).to_pylist()
            )
            if not seen:
                seen = rows
            assert rows == seen, (tag_order, layout, orchestrator.__name__)


def test_the_output_column_order_follows_the_input() -> None:
    """Presentation only: the keys keep the input's tag order."""
    forward = _input("plain", "source", ("g", "k", "x", "y"), LAYOUTS[0])
    backward = _input("plain", "source", ("k", "g", "y", "x"), LAYOUTS[3])
    assert forward.group_by(["g", "k"]).keys() == (("g", "k"), ("a", "b"))
    assert backward.group_by(["g", "k"]).keys() == (("k", "g"), ("b", "a"))


@pytest.mark.parametrize("member_order", [None, ["y"]])
@pytest.mark.parametrize("mode", MODES)
def test_the_order_of_by_is_not_identity(
    mode: str, member_order: list[str] | None
) -> None:
    """The keys' order only ordered the groups (rows): it is not identity.

    Developer, November 17, 2026: ``GroupBy(["g", "k"])`` and ``GroupBy(["k", "g"])``
    are one operator: equal identity, config, hashes, rows and row order, whatever
    the arrangement of the input.
    """
    seen: dict[str, Any] = {}
    for by, tag_order, layout in itertools.product(
        (["g", "k"], ["k", "g"], {"k", "g"}), (TAGS, ("y", "k", "x", "g")), LAYOUTS
    ):
        group_by = GroupBy(by, member_order=member_order)
        grouped = group_by(_input(mode, "source", tag_order, layout))
        found = {
            "identity": group_by.identity_structure(),
            "config": group_by.to_config(),
            "label": grouped.label,
            "row_order": [(tag["g"], tag["k"]) for tag, _ in grouped.iter_data()],
            **_fingerprint(grouped, content_hash=True),
        }
        if not seen:
            seen = found
        assert found == seen, (by, tag_order, layout)
    assert seen["config"]["config"]["by"] == ["g", "k"]


def test_groups_are_emitted_by_their_keys_in_name_order() -> None:
    rows = [{"g": g, "k": k, "a": float(10 * g + k)} for g in (2, 1) for k in (1, 2)]
    source = op.sources.DictSource(
        rows, tag_columns=["k", "g"], tag_only_columns=True, source_id="emit"
    )
    for by in (["k", "g"], ["g", "k"]):
        grouped = source.group_by(by)
        assert [(t["g"], t["k"]) for t, _ in grouped.iter_data()] == [
            (1, 1),
            (1, 2),
            (2, 1),
            (2, 2),
        ]


# ── the order itself ────────────────────────────────────────────────────────


def _a_lists(stream: StreamProtocol) -> dict[object, object]:
    return {tag["k"]: data["a"] for tag, data in stream.iter_data()}


@pytest.mark.parametrize("tag_order", [TAGS, ("g", "k", "y", "x")])
def test_by_default_members_are_ordered_by_the_non_key_tags_in_name_order(
    tag_order: tuple[str, ...],
) -> None:
    grouped = _input("plain", "source", tag_order, LAYOUTS[0]).group_by(["g", "k"])
    # By ``x``, then ``y`` (nulls last), then the record id (rows 1 and 2 tie).
    assert _a_lists(grouped) == {1: [0.2, 0.3, 0.1], 2: [0.6, 0.4, 0.5]}


@pytest.mark.parametrize("tag_order", [TAGS, ("g", "k", "y", "x")])
def test_member_order_names_the_tags_that_order_the_members(
    tag_order: tuple[str, ...],
) -> None:
    source = _input("plain", "source", tag_order, LAYOUTS[0])
    # By ``y`` (nulls last), then ``x``, then the record id.
    expected = {1: [0.1, 0.2, 0.3], 2: [0.5, 0.6, 0.4]}
    assert _a_lists(source.group_by(["g", "k"], member_order=["y"])) == expected
    assert _a_lists(source.group_by(["g", "k"], member_order=["y", "x"])) == expected
    # Naming them in name order is the default order, under its own identity.
    assert _a_lists(source.group_by(["g", "k"], member_order=["x", "y"])) == {
        1: [0.2, 0.3, 0.1],
        2: [0.6, 0.4, 0.5],
    }


def test_a_data_tag_orders_like_a_tag_and_its_list_follows_the_members() -> None:
    grouped = _input("data_tags", "source", TAGS, LAYOUTS[0]).group_by(
        ["g", "k"], member_order=["y"]
    )
    rows = {tag["k"]: data for tag, data in grouped.iter_data()}
    assert rows[1]["y"] == [1, 2, 2] and rows[1]["x"] == [2, 1, 1]
    assert rows[2]["y"] == [1, 3, None] and rows[2]["x"] == [2, 1, 1]


def test_a_grouped_stream_grouped_again_orders_by_the_remaining_keys() -> None:
    rows = [
        {"g": 1, "k": k, "x": x, "a": float(10 * k + x)}
        for k in (2, 1, 3)
        for x in (1, 2)
    ]
    source = op.sources.DictSource(
        rows, tag_columns=["g", "k", "x"], tag_only_columns=True, source_id="nested"
    )
    twice = source.group_by(["g", "k"]).group_by(["g"])
    ((_, data),) = list(twice.iter_data())
    assert data["a"] == [[11.0, 12.0], [21.0, 22.0], [31.0, 32.0]]


# ── identity ────────────────────────────────────────────────────────────────


def test_without_member_order_the_identity_is_unchanged() -> None:
    assert GroupBy(["k"]).identity_structure() == ("GroupBy", ("k",))
    assert GroupBy(["k"], member_order="name").identity_structure() == (
        "GroupBy",
        ("k",),
    )
    assert "member_order" not in GroupBy(["k"]).to_config()["config"]


def test_member_order_is_part_of_the_identity() -> None:
    source = _input("plain", "source", TAGS, LAYOUTS[0])
    default = source.group_by(["g", "k"])
    by_y = source.group_by(["g", "k"], member_order=["y"])
    by_x = source.group_by(["g", "k"], member_order=["x"])
    hashes = {s.content_hash() for s in (default, by_y, by_x)}
    assert len(hashes) == 3
    assert len({s.pipeline_hash() for s in (default, by_y, by_x)}) == 3
    assert GroupBy(["k"], member_order=["y", "x"]).identity_structure() == (
        "GroupBy",
        ("k",),
        ("member_order", ("y", "x")),
    )
    # The keyword ``name`` is the default, identity included.
    named = source.group_by(["g", "k"], member_order="name")
    assert named.content_hash() == default.content_hash()


@pytest.mark.parametrize("member_order", [None, ["y", "x"]])
def test_the_config_round_trips(member_order: list[str] | None) -> None:
    group_by = GroupBy(["g", "k"], member_order=member_order)
    config = group_by.to_config()
    if member_order is None:
        assert config["config"] == {"by": ["g", "k"]}
    else:
        assert config["config"] == {"by": ["g", "k"], "member_order": member_order}
    rebuilt = resolve_operator_from_config(config)
    assert isinstance(rebuilt, GroupBy)
    assert rebuilt.identity_structure() == group_by.identity_structure()


# ── refusals ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("member_order", "error", "match"),
    [
        ([], ValueError, "at least one"),
        (["y", "y"], ValueError, "more than once"),
        ({"x", "y"}, TypeError, "not a set"),
        ([1], TypeError, "strings"),
        (["k"], ValueError, "group key"),
        ("canonical", ValueError, "'name'"),
        ("x", ValueError, r"a list of tag names: member_order=\['x'\]"),
    ],
    ids=[
        "empty",
        "repeated",
        "set",
        "not_a_string",
        "key",
        "unknown_keyword",
        "bare_tag_name",
    ],
)
def test_bad_member_orders_are_refused_at_construction(
    member_order: Any,  # pyright: ignore[reportExplicitAny]  # the refusal is the test
    error: type[Exception],
    match: str,
) -> None:
    with pytest.raises(error, match=match):
        _ = GroupBy(["g", "k"], member_order=member_order)


def test_an_ordering_function_is_not_available_yet() -> None:
    order: Callable[[frozenset[str]], list[str]] = sorted
    with pytest.raises(NotImplementedError, match="ITL-1226"):
        _ = GroupBy(["k"], member_order=order)  # pyright: ignore[reportArgumentType]  # reserved form


@pytest.mark.parametrize(
    ("name", "match"),
    [
        ("a", r"'a', which is a data column.*tag_by\('a'\)"),
        ("nope", "'nope', which is not a tag"),
    ],
)
def test_a_name_that_is_not_a_tag_of_the_input_is_refused(
    name: str, match: str
) -> None:
    source = _input("plain", "source", TAGS, LAYOUTS[0])
    with pytest.raises(InputValidationError, match=match):
        _ = source.group_by(["g", "k"], member_order=[name])
