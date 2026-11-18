"""Sources with several tag, key and data columns, in several orders and roles (ITL-1236).

The rules tested (CLAUDE.md, "Important implementation details" and "Two identity
chains"):

* The order of ``tag_columns`` is the order of the tags (and of the data-tags among the
  data columns); it enters no hash, no record id and no system-tag column name.
* ``tag_only_columns`` names a set: its order changes nothing.
* ``record_id_columns`` is in key order: a record id is the canonical JSON of the key
  columns in key order, so another key order is another source with the same pipeline
  hash and source id, another content hash. An unordered collection is refused.
* ``DerivedSource(tags_as_data=...)``: the entries' order is the order of the new data
  columns and nothing else.
* ``CachedSource`` reads back a source of each role as the source itself.
"""

from __future__ import annotations

import itertools
import json

import pyarrow as pa
import pytest

from orcapod.core.sources import (
    ArrowTableSource,
    CachedSource,
    DerivedSource,
    DictSource,
)
from orcapod.databases import InMemoryArrowDatabase

from ..operators.several_items import (
    DATA_TAGS,
    LAYOUTS,
    MAIN,
    MIXED,
    PLAIN,
    ROLES,
    TAGS_FIRST,
    Arrangement,
    fingerprint,
    make_source,
    presentation,
)


def _tag_only(role: str) -> bool | list[str]:
    return {PLAIN: True, DATA_TAGS: [], MIXED: [MAIN.mixed_plain]}[role]


def _table() -> pa.Table:
    return pa.table({name: MAIN.columns[name] for name in (*MAIN.tags, *MAIN.data)})


def _record_ids(source: ArrowTableSource) -> list[str]:
    ids: list[str] = []
    for tag, _ in source.iter_data():
        [record_id] = [
            value for name, value in tag.system_tags().items() if "record_id" in name
        ]
        ids.append(str(record_id))
    return sorted(ids)


@pytest.mark.parametrize("role", ROLES)
def test_tag_order_is_the_order_of_the_tags_and_enters_no_hash(role: str) -> None:
    """Every tag order and layout: same rows, hashes and system tags; keys follow."""
    results = {}
    for order in itertools.permutations(MAIN.tags):
        for layout in LAYOUTS:
            source = make_source(MAIN, Arrangement(order, layout, role))
            tag_keys, data_keys = presentation(source)
            assert tag_keys == order
            # Data-tags come first among the data columns, in tag order (ITL-1205).
            data_tags = [t for t in order if role != PLAIN and t != _plain_tag(role)]
            assert list(data_keys[: len(data_tags)]) == data_tags
            results[(order, layout)] = fingerprint(source)
    first = next(iter(results.values()))
    assert all(result == first for result in results.values())


def _plain_tag(role: str) -> str | None:
    return MAIN.mixed_plain if role == MIXED else None


def test_the_order_of_tag_only_columns_changes_nothing() -> None:
    sources = [
        ArrowTableSource(
            _table(), tag_columns=list(MAIN.tags), source_id="s", tag_only_columns=names
        )
        for names in (["t1", "t3"], ["t3", "t1"])
    ]
    assert presentation(sources[0]) == presentation(sources[1])
    assert presentation(sources[0]) == (("t1", "t2", "t3"), ("t2", "d1", "d2", "d3"))
    assert fingerprint(sources[0]) == fingerprint(sources[1])


@pytest.mark.parametrize("role", ROLES)
def test_record_ids_follow_the_key_order(role: str) -> None:
    """Two or three key columns, of the tag and the data role, in several orders."""
    by_keys = {}
    for keys in (("t1", "t2", "t3"), ("t3", "t1", "t2"), ("d3", "t1"), ("t1", "d3")):
        source = ArrowTableSource(
            _table(),
            tag_columns=list(MAIN.tags),
            source_id="s",
            record_id_columns=list(keys),
            tag_only_columns=_tag_only(role),
        )
        ids = _record_ids(source)
        # The canonical JSON of the key columns, in key order.
        assert all(list(json.loads(record_id)) == list(keys) for record_id in ids)
        by_keys[keys] = source
    first = by_keys[("t1", "t2", "t3")]
    for keys, source in by_keys.items():
        # The keys name the rows: one pipeline hash and one source id ...
        assert source.pipeline_hash() == first.pipeline_hash()
        assert source.source_id == first.source_id
        # ... and another key order is another source.
        if keys != ("t1", "t2", "t3"):
            assert source.content_hash() != first.content_hash()
            assert _record_ids(source) != _record_ids(first)
    # The same key columns in another order, compared with each other.
    for one, other in (
        (("t1", "t2", "t3"), ("t3", "t1", "t2")),
        (("d3", "t1"), ("t1", "d3")),
    ):
        assert by_keys[one].content_hash() != by_keys[other].content_hash()
        assert by_keys[one].pipeline_hash() == by_keys[other].pipeline_hash()
        assert _record_ids(by_keys[one]) != _record_ids(by_keys[other])
    # Same keys, same order: the same source.
    again = ArrowTableSource(
        _table(),
        tag_columns=list(MAIN.tags),
        source_id="s",
        record_id_columns=("t3", "t1", "t2"),
        tag_only_columns=_tag_only(role),
    )
    assert again.content_hash() == by_keys[("t3", "t1", "t2")].content_hash()


def test_record_id_columns_as_a_set_is_refused() -> None:
    with pytest.raises(TypeError):
        ArrowTableSource(
            _table(),
            tag_columns=list(MAIN.tags),
            source_id="s",
            record_id_columns={"t1", "t2"},
        )


@pytest.mark.parametrize("role", ROLES)
def test_tags_as_data_order_is_the_order_of_the_new_columns_only(role: str) -> None:
    source = make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, role))
    forward = DerivedSource(source, tags_as_data={"t1": "c1", "t3": "c3"})
    backward = DerivedSource(source, tags_as_data={"t3": "c3", "t1": "c1"})
    assert presentation(forward)[1][-2:] == ("c1", "c3")
    assert presentation(backward)[1][-2:] == ("c3", "c1")
    assert forward.source_id == backward.source_id
    assert fingerprint(forward) == fingerprint(backward)
    # Each new column holds its own tag's values.
    for tag, data in forward.iter_data():
        assert (data["c1"], data["c3"]) == (tag["t1"], tag["t3"])


@pytest.mark.parametrize("role", ROLES)
def test_a_cached_source_reads_back_every_role_and_arrangement(
    role: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    for order in MAIN.tag_orders():
        for layout in LAYOUTS:
            arrangement = Arrangement(order, layout, role)
            database = InMemoryArrowDatabase()
            cached = CachedSource(
                make_source(MAIN, arrangement), cache_database=database
            )
            assert fingerprint(cached) == fingerprint(make_source(MAIN, arrangement))
            # A second CachedSource over the same source (same schema and identity)
            # that cannot provide rows: it serves the stored rows only.
            unavailable = make_source(MAIN, arrangement)

            def no_rows(*args: object, **kwargs: object) -> pa.Table:
                raise NotImplementedError("rows are not available")

            monkeypatch.setattr(unavailable, "as_table", no_rows)
            again = CachedSource(unavailable, cache_database=database)
            assert presentation(again) == presentation(make_source(MAIN, arrangement))
            assert fingerprint(again) == fingerprint(cached)


@pytest.mark.parametrize(
    "data_schema",
    [
        {"t1": str, "t2": int, "d1": int, "d3": str},
        {"d3": str, "t2": int, "d1": int, "t1": str},
    ],
    ids=["table_order", "another_order"],
)
@pytest.mark.parametrize("role", ROLES)
def test_a_schema_in_another_field_order_gives_the_same_source(
    data_schema: dict[str, type], role: str
) -> None:
    """A ``DictSource`` schema with several fields, in the rows' order or another."""
    rows = [
        {"t1": "a", "t2": 1, "d1": 10, "d3": "x"},
        {"t1": "b", "t2": 2, "d1": 20, "d3": "y"},
    ]
    tag_only = {PLAIN: True, DATA_TAGS: [], MIXED: ["t2"]}[role]
    source = DictSource(
        rows,
        tag_columns=["t1", "t2"],
        source_id="d",
        data_schema=data_schema,
        tag_only_columns=tag_only,
    )
    reference = DictSource(
        rows,
        tag_columns=["t1", "t2"],
        source_id="d",
        data_schema={"t1": str, "t2": int, "d1": int, "d3": str},
        tag_only_columns=tag_only,
    )
    assert fingerprint(source) == fingerprint(reference)
    assert presentation(source)[0] == ("t1", "t2")
