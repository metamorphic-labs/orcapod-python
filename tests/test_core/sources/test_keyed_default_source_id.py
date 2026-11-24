"""``hash_in_key_order`` and the identity of ``ListSource`` (ITL-1272).

Developer decisions, November 21, 2026:

* "as for sorting by key columns or not prior to hashing, we actually should take it
  explicitly as a parameter rather than always hashing the sorted version of the data.
  The user might have had a reason to put the data in the specific order per se." An
  in-memory source with key columns and no ``source_id`` hashes its table, for its default
  source id, sorted by the key columns in key order when given ``hash_in_key_order=True``;
  by default it hashes the table as given. The argument is refused without key columns
  and with a ``source_id``. The rows are presented in the order given either way.
* "Content hash of the list should factor into the content hash of the source only if
  source_id is NOT given." ``ListSource`` has the identity of every root source.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Sequence
from typing import Any, cast

import polars as pl
import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import (
    ArrowTableSource,
    DataFrameSource,
    DictSource,
    ListSource,
    RootSource,
)
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import RowIdentifierError
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION, constants
from orcapod.utils import arrow_utils

Row = dict[str, Any]

# Two tag columns and two data columns; no single tag names a row, so a key is
# ("subject", "trial") or ("trial", "subject"). Weights are unique: a ListSource's
# element is the weight, and its tags are looked up from it.
ROWS: list[Row] = [
    {"subject": "s1", "trial": 1, "weight": 20.5, "count": 3},
    {"subject": "s1", "trial": 2, "weight": 22.25, "count": 1},
    {"subject": "s2", "trial": 1, "weight": 19.0, "count": 4},
    {"subject": "s2", "trial": 2, "weight": 21.75, "count": 2},
]
SCHEMA = pa.schema(
    [
        pa.field("subject", pa.large_string(), nullable=False),
        pa.field("trial", pa.int64(), nullable=False),
        pa.field("weight", pa.float64(), nullable=False),
        pa.field("count", pa.int64(), nullable=False),
    ]
)
TAGS = ["subject", "trial"]

# Row orders: as given, reversed, and two interleavings (none is the key order of both
# key orders at once). ROW_ORDERS[0] is the key order of ("subject", "trial").
ROW_ORDERS: list[tuple[int, ...]] = [
    (0, 1, 2, 3),
    (3, 2, 1, 0),
    (2, 0, 3, 1),
    (1, 3, 0, 2),
]
KEY_ORDERS: list[list[str]] = [["subject", "trial"], ["trial", "subject"]]
KEY_IDS = ["subject_trial", "trial_subject"]
KINDS = ["arrow_table", "dict", "data_frame", "list"]
# Tag-only (both tags plain) and data-tag (both tags also data) forms. A ListSource
# takes no tag_only_columns: its tags are plain.
ROLES: dict[str, Any] = {"tag_only": True, "data_tag": ()}


def _ordered(order: Sequence[int], rows: list[Row] = ROWS) -> list[Row]:
    return [rows[i] for i in order]


def make(kind: str, rows: list[Row], role: str = "tag_only", **kw: Any) -> RootSource:
    """A source of ``kind`` over ``rows``, in the order given."""
    if kind == "list":
        by_weight = {row["weight"]: row for row in rows}
        return ListSource(
            "weight",
            [row["weight"] for row in rows],
            tag_function=lambda w, _i: {t: by_weight[w][t] for t in TAGS},
            expected_tag_keys=TAGS,
            **kw,
        )
    tag_only = ROLES[role]
    if kind == "arrow_table":
        has_null = any(value is None for row in rows for value in row.values())
        schema = pa.schema([f.with_nullable(has_null) for f in SCHEMA])
        table = pa.Table.from_pylist(rows, schema=schema)
        return ArrowTableSource(
            table, tag_columns=TAGS, tag_only_columns=tag_only, **kw
        )
    if kind == "dict":
        return DictSource(rows, tag_columns=TAGS, tag_only_columns=tag_only, **kw)
    return DataFrameSource(
        pl.DataFrame(rows), tag_columns=TAGS, tag_only_columns=tag_only, **kw
    )


CASES = [(k, r) for k in KINDS for r in ROLES if not (k == "list" and r == "data_tag")]
CASE_IDS = [f"{k}-{r}" for k, r in CASES]


def _column_values(source: RootSource, prefix: str) -> list[str]:
    table = source.as_table(all_info=True)
    (column,) = [c for c in table.column_names if c.startswith(prefix)]
    return cast("list[str]", table.column(column).to_pylist())


def _tokens(source: RootSource, column: str) -> set[str]:
    table = source.as_table(all_info=True)
    values = table.column(f"{constants.SOURCE_PREFIX}{column}").to_pylist()
    return set(cast("list[str]", values))


@pytest.mark.parametrize(("kind", "role"), CASES, ids=CASE_IDS)
class TestHashInKeyOrder:
    @pytest.mark.parametrize("key", KEY_ORDERS, ids=KEY_IDS)
    def test_by_default_the_rows_are_hashed_as_given(
        self, kind: str, role: str, key: list[str]
    ) -> None:
        first, second = (
            make(kind, _ordered(order), role, record_id_columns=key)
            for order in ROW_ORDERS[:2]
        )
        assert first.source_id != second.source_id
        assert first.content_hash() != second.content_hash()

    @pytest.mark.parametrize("key", KEY_ORDERS, ids=KEY_IDS)
    def test_in_key_order_every_row_order_gives_one_id_and_one_content_hash(
        self, kind: str, role: str, key: list[str]
    ) -> None:
        sources = [
            make(
                kind,
                _ordered(order),
                role,
                record_id_columns=key,
                hash_in_key_order=True,
            )
            for order in ROW_ORDERS
        ]
        assert len({s.source_id for s in sources}) == 1
        assert sources[0].source_id.startswith(f"{kind}_v1:")
        assert len({s.content_hash() for s in sources}) == 1
        assert len({s.pipeline_hash() for s in sources}) == 1

    def test_in_key_order_the_key_order_is_part_of_the_identity(
        self, kind: str, role: str
    ) -> None:
        """Key orders that sort the table differently: two ids and two content hashes."""
        first, second = (
            make(
                kind,
                _ordered(ROW_ORDERS[2]),
                role,
                record_id_columns=key,
                hash_in_key_order=True,
            )
            for key in KEY_ORDERS
        )
        assert first.source_id != second.source_id
        assert first.content_hash() != second.content_hash()

    def test_key_orders_that_sort_alike_share_the_id_not_the_content_hash(
        self, kind: str, role: str
    ) -> None:
        """Both key orders sort these rows alike: one default id, two content hashes."""
        # subject and trial rise together, so sorting by (subject, trial) and by
        # (trial, subject) gives one table.
        rows = [
            {"subject": "s2", "trial": 2, "weight": 22.25, "count": 1},
            {"subject": "s3", "trial": 3, "weight": 19.0, "count": 4},
            {"subject": "s1", "trial": 1, "weight": 20.5, "count": 3},
        ]
        first, second = (
            make(kind, rows, role, record_id_columns=key, hash_in_key_order=True)
            for key in KEY_ORDERS
        )
        assert first.source_id == second.source_id
        assert first.content_hash() != second.content_hash()
        assert first.pipeline_hash() == second.pipeline_hash()

    def test_rows_already_in_key_order_hash_as_given(
        self, kind: str, role: str
    ) -> None:
        rows = _ordered(ROW_ORDERS[0])
        as_given = make(kind, rows, role, record_id_columns=TAGS)
        in_key_order = make(
            kind, rows, role, record_id_columns=TAGS, hash_in_key_order=True
        )
        assert in_key_order.source_id == as_given.source_id

    @pytest.mark.parametrize("no_key", [None, ()], ids=["default", "position"])
    def test_refused_without_key_columns(
        self, kind: str, role: str, no_key: Any
    ) -> None:
        with pytest.raises(ValueError, match="hash_in_key_order=True needs key"):
            make(kind, ROWS, role, record_id_columns=no_key, hash_in_key_order=True)

    def test_refused_with_a_source_id(self, kind: str, role: str) -> None:
        with pytest.raises(ValueError, match="hash_in_key_order=True.*source_id"):
            make(
                kind,
                ROWS,
                role,
                record_id_columns=TAGS,
                source_id="given",
                hash_in_key_order=True,
            )

    @pytest.mark.parametrize("key", KEY_ORDERS, ids=KEY_IDS)
    def test_system_tags_and_source_info_follow_the_rows(
        self, kind: str, role: str, key: list[str]
    ) -> None:
        sources = [
            make(
                kind,
                _ordered(order),
                role,
                record_id_columns=key,
                hash_in_key_order=True,
            )
            for order in ROW_ORDERS
        ]
        source_ids = [
            set(_column_values(s, constants.SYSTEM_TAG_SOURCE_ID_PREFIX))
            for s in sources
        ]
        assert source_ids == [{sources[0].source_id}] * len(sources)
        record_ids = [
            set(_column_values(s, constants.SYSTEM_TAG_RECORD_ID_PREFIX))
            for s in sources
        ]
        assert all(r == record_ids[0] for r in record_ids)
        for column in ("weight", "count") if kind != "list" else ("weight",):
            tokens = [_tokens(s, column) for s in sources]
            assert all(t == tokens[0] for t in tokens)
            assert all(
                t.startswith(sources[0].source_id + constants.BLOCK_SEPARATOR)
                for t in tokens[0]
            )

    def test_the_rows_are_presented_in_the_order_given(
        self, kind: str, role: str
    ) -> None:
        for order in ROW_ORDERS[1:3]:
            source = make(
                kind,
                _ordered(order),
                role,
                record_id_columns=TAGS,
                hash_in_key_order=True,
            )
            assert source.as_table().column("weight").to_pylist() == [
                ROWS[i]["weight"] for i in order
            ]

    def test_a_given_source_id_is_an_address(self, kind: str, role: str) -> None:
        """Rows reordered or changed under one ``source_id``: one content hash."""
        changed = [dict(ROWS[0], weight=99.5, count=7), *ROWS[1:]]
        variants = [_ordered(order) for order in ROW_ORDERS[:3]] + [changed]
        hashes = {
            make(
                kind, rows, role, record_id_columns=TAGS, source_id="given"
            ).content_hash()
            for rows in variants
        }
        assert len(hashes) == 1
        other = make(kind, ROWS, role, record_id_columns=TAGS, source_id="other")
        assert other.content_hash() not in hashes


class TestHashInKeyOrderIsTheKeySortedTableHash:
    @pytest.mark.parametrize("key", KEY_ORDERS, ids=KEY_IDS)
    def test_default_is_the_hash_of_the_table_in_key_order(
        self, key: list[str]
    ) -> None:
        table = pa.Table.from_pylist(_ordered(ROW_ORDERS[2]), schema=SCHEMA)
        source = ArrowTableSource(
            table,
            tag_columns=TAGS,
            tag_only_columns=True,
            record_id_columns=key,
            hash_in_key_order=True,
        )
        hasher = source.data_context.arrow_hasher
        expected = hasher.hash_table(table.sort_by([(c, "ascending") for c in key]))
        assert source.source_id == f"arrow_table_v1:{expected.to_string()}"

    def test_without_it_the_default_is_the_hash_of_the_table_as_given(self) -> None:
        table = pa.Table.from_pylist(_ordered(ROW_ORDERS[2]), schema=SCHEMA)
        source = ArrowTableSource(
            table, tag_columns=TAGS, tag_only_columns=True, record_id_columns=TAGS
        )
        expected = source.data_context.arrow_hasher.hash_table(table)
        assert source.source_id == f"arrow_table_v1:{expected.to_string()}"


@pytest.mark.parametrize("kind", KINDS)
class TestBadKeysAreRefusedBeforeHashing:
    """A repeated or null key is refused before anything is sorted or hashed."""

    @pytest.fixture(autouse=True)
    def _no_hashing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def refuse(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("hashed or sorted before the keys were checked")

        hasher = DictSource(ROWS, tag_columns=TAGS).data_context.arrow_hasher
        monkeypatch.setattr(arrow_utils, "key_row_order", refuse)
        monkeypatch.setattr(type(hasher), "hash_table", refuse)

    def test_a_repeated_key_is_refused(self, kind: str) -> None:
        rows = [dict(ROWS[0]), dict(ROWS[1]), dict(ROWS[0], weight=99.0)]
        with pytest.raises(RowIdentifierError, match="must be unique"):
            make(kind, rows, record_id_columns=TAGS, hash_in_key_order=True)

    def test_a_null_key_is_refused(self, kind: str) -> None:
        rows = [dict(ROWS[0]), dict(ROWS[1], trial=None)]
        with pytest.raises(RowIdentifierError, match="null"):
            make(kind, rows, record_id_columns=TAGS, hash_in_key_order=True)


@pytest.mark.parametrize("kind", KINDS)
class TestEveryKindAcceptsKeyColumnsTheSameWay:
    """``record_id_columns`` means the same thing for every in-memory source kind."""

    @pytest.mark.parametrize(
        "key",
        [
            "subject",
            ["subject", "trial"],
            ("trial", "subject"),
            (),
            None,
        ],
        ids=["name", "list", "tuple", "position", "default"],
    )
    def test_the_same_record_ids_for_every_kind(self, kind: str, key: Any) -> None:
        # Unique subjects, so "subject" alone is a key too.
        rows = [dict(r, subject=f"{r['subject']}_{r['trial']}") for r in ROWS]
        expected = make("dict", _ordered(ROW_ORDERS[2], rows), record_id_columns=key)
        source = make(kind, _ordered(ROW_ORDERS[2], rows), record_id_columns=key)
        assert _column_values(
            source, constants.SYSTEM_TAG_RECORD_ID_PREFIX
        ) == _column_values(expected, constants.SYSTEM_TAG_RECORD_ID_PREFIX)

    def test_a_set_of_key_columns_is_refused(self, kind: str) -> None:
        with pytest.raises(TypeError, match="key order"):
            make(kind, ROWS, record_id_columns={"subject", "trial"})


# Key column types: string, integer, boolean, timestamp (naive and zoned), date32 and
# date64, each with values given out of order.
KEY_TYPES: dict[str, "pa.Array[Any]"] = {
    "string": pa.array(["c", "a", "b"], pa.large_string()),
    "integer": pa.array([3, -1, 2], pa.int32()),
    "boolean": pa.array([True, False], pa.bool_()),
    "timestamp": pa.array(
        [dt.datetime(2026, 1, 3), dt.datetime(1969, 12, 31), dt.datetime(2026, 1, 2)],
        pa.timestamp("us"),
    ),
    "timestamp_utc": pa.array(
        [
            dt.datetime(2026, 1, 3, tzinfo=dt.timezone.utc),
            dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc),
            dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc),
        ],
        pa.timestamp("ns", tz="UTC"),
    ),
    "date32": pa.array(
        [dt.date(2026, 1, 3), dt.date(2000, 2, 29), dt.date(2026, 1, 2)], pa.date32()
    ),
    "date64": pa.array(
        [dt.date(2026, 1, 3), dt.date(2000, 2, 29), dt.date(2026, 1, 2)], pa.date64()
    ),
}


@pytest.mark.parametrize("type_name", KEY_TYPES, ids=list(KEY_TYPES))
class TestEveryKeyColumnType:
    def _source(self, type_name: str, order: Sequence[int], **kw: Any) -> RootSource:
        key = KEY_TYPES[type_name]
        values = pa.array([float(i) for i in range(len(key))], pa.float64())
        table = pa.table({"key": key, "value": values})
        return ArrowTableSource(
            table.take(pa.array(list(order), pa.int64())),
            tag_columns=["key"],
            tag_only_columns=True,
            record_id_columns="key",
            **kw,
        )

    def _orders(self, type_name: str) -> list[list[int]]:
        n = len(KEY_TYPES[type_name])
        return [[0, 1], [1, 0]] if n == 2 else [[0, 1, 2], [2, 1, 0], [1, 2, 0]]

    def test_in_key_order_one_id_for_every_row_order(self, type_name: str) -> None:
        sources = [
            self._source(type_name, order, hash_in_key_order=True)
            for order in self._orders(type_name)
        ]
        assert len({s.source_id for s in sources}) == 1
        assert len({s.content_hash() for s in sources}) == 1

    def test_as_given_the_ids_follow_the_row_order(self, type_name: str) -> None:
        first, second = (
            self._source(type_name, order) for order in self._orders(type_name)[:2]
        )
        assert first.source_id != second.source_id


class TestListSourceIdentity:
    """``ListSource`` has the identity of every root source (ITL-1272)."""

    def test_the_identity_is_that_of_every_root_source(self) -> None:
        for key, resolved in ((TAGS, tuple(TAGS)), (None, ())):
            source = make("list", ROWS, record_id_columns=key, source_id="given")
            identity = source.identity_structure()
            assert identity == RootSource.identity_structure(source)
            assert identity[0] == "list_v1:given"
            assert identity[2:] == (resolved,)

    def test_with_a_source_id_the_elements_do_not_count(self) -> None:
        hashes = {
            make("list", rows, record_id_columns=TAGS, source_id="given").content_hash()
            for rows in (ROWS, ROWS[::-1], ROWS[:3], [dict(ROWS[0], weight=99.5)])
        }
        assert len(hashes) == 1

    def test_with_a_source_id_a_tag_function_giving_the_same_tags_does_not_count(
        self,
    ) -> None:
        by_weight = {row["weight"]: row for row in ROWS}

        def same_tags(weight: float, index: int) -> dict[str, Any]:
            """Another function (keys built in another order) giving the same tags."""
            del index
            row = by_weight[weight]
            return {"trial": row["trial"], "subject": row["subject"]}

        other = ListSource(
            "weight",
            [row["weight"] for row in ROWS],
            tag_function=same_tags,
            expected_tag_keys=TAGS,
            record_id_columns=TAGS,
            source_id="given",
        )
        plain = make("list", ROWS, record_id_columns=TAGS, source_id="given")
        assert plain.content_hash() == other.content_hash()

    def test_the_source_id_counts(self) -> None:
        first, second = (
            make("list", ROWS, record_id_columns=TAGS, source_id=name)
            for name in ("first", "second")
        )
        assert first.content_hash() != second.content_hash()

    def test_without_a_source_id_a_changed_element_counts(self) -> None:
        changed = [dict(ROWS[0], weight=99.5), *ROWS[1:]]
        assert (
            make("list", ROWS, record_id_columns=TAGS).content_hash()
            != make("list", changed, record_id_columns=TAGS).content_hash()
        )

    def test_tag_function_hash_mode_is_gone(self) -> None:
        with pytest.raises(TypeError):
            ListSource(
                "weight",
                [1.0],
                tag_function_hash_mode="name",  # pyright: ignore[reportCallIssue]
            )


def _pipeline_table(database: InMemoryArrowDatabase) -> pa.Table:
    """The one function pipeline table in ``database``."""
    (path,) = [
        p for p in database.list_table_paths() if p[-1] == PIPELINE_DB_SCHEMA_VERSION
    ]
    stored = database.get_all_records(path)
    assert stored is not None
    return stored


def doubled(weight: float) -> float:
    return weight * 2


def scaled(weight: float, count: int) -> float:
    return weight * count


def scaled_with_tags(subject: str, trial: int, weight: float, count: int) -> float:
    """``scaled`` for a data-tag source, whose data-tags are data too."""
    return weight * count


def _pod_function(kind: str, role: str) -> Callable[..., float]:
    if kind == "list":
        return doubled
    return scaled if role == "tag_only" else scaled_with_tags


@pytest.mark.parametrize(("kind", "role"), CASES, ids=CASE_IDS)
class TestJobNodeOverAKeyOrderedSource:
    def test_every_row_order_stores_under_one_path_and_one_node(
        self, kind: str, role: str
    ) -> None:
        pod = FunctionPod.from_fn(_pod_function(kind, role), output_keys="out")
        database = InMemoryArrowDatabase()
        nodes = []
        for order in ROW_ORDERS[:3]:
            source = make(
                kind,
                _ordered(order),
                role,
                record_id_columns=KEY_ORDERS[1],
                hash_in_key_order=True,
            )
            node = FunctionJobNode(
                function_pod=pod, input_stream=source, pipeline_database=database
            )
            node.run()
            nodes.append(node)
        assert len({n.node_identity_path for n in nodes}) == 1
        assert len({n.content_hash() for n in nodes}) == 1
        stored = _pipeline_table(database)
        # One entry per row: the later row orders found every entry stored.
        assert stored.num_rows == len(ROWS)
        node_hashes = set(stored.column(constants.NODE_CONTENT_HASH_COL).to_pylist())
        assert len(node_hashes) == 1

    def test_as_given_two_row_orders_are_two_nodes(self, kind: str, role: str) -> None:
        pod = FunctionPod.from_fn(_pod_function(kind, role), output_keys="out")
        first, second = (
            FunctionJobNode(
                function_pod=pod,
                input_stream=make(kind, _ordered(order), role, record_id_columns=TAGS),
                pipeline_database=InMemoryArrowDatabase(),
            )
            for order in ROW_ORDERS[:2]
        )
        assert first.node_identity_path == second.node_identity_path
        assert first.content_hash() != second.content_hash()


def test_build_takes_hash_in_key_order_by_keyword_only() -> None:
    """``SourceStreamBuilder.build`` refuses ``hash_in_key_order`` given by position."""
    from orcapod import DEFAULT_CONFIG
    from orcapod.contexts import get_default_context
    from orcapod.core.sources.stream_builder import SourceStreamBuilder

    builder = SourceStreamBuilder(get_default_context(), DEFAULT_CONFIG)
    table = pa.Table.from_pylist(ROWS, schema=SCHEMA)
    positional: Any = builder.build  # the signature refuses this; call it untyped
    with pytest.raises(TypeError):
        positional(table, TAGS, None, TAGS, (), "arrow_table_v1", (), True)
    result = builder.build(
        table,
        TAGS,
        None,
        TAGS,
        (),
        "arrow_table_v1",
        (),
        hash_in_key_order=True,
    )
    assert result.source_id.startswith("arrow_table_v1:")
