"""A source-info token column keeps the pipeline's type on every route (ITL-1491).

The Arrow type of a token column is determined by the pipeline, never taken from the
tokens' values: a source builds ``large_string`` tokens, each operator determines its
output's token types from its inputs' types, and a function node takes the types from
its input's rows. So a job stores the same rows, token types and ids (entry, instance,
record) on the sync and on the async orchestrator, also for token columns whose
values cannot be typed again from Python values (a null or an empty list inside a
list), which used to raise at the sync edge between two nodes while the async route
recorded them.

Every source has two tag columns and two data columns, three or four rows, in two
arrangements (tag order and data order both reversed).
"""

from __future__ import annotations

import uuid
from collections import Counter
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.core import row_ids
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import Batch, GroupBy, MergeJoin
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.system_constants import constants
from orcapod.types import ContentHash

HASHER = get_default_context().arrow_hasher
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}
ARRANGEMENTS = {
    "as_declared": (("subject", "session"), ("x", "w")),
    "reversed": (("session", "subject"), ("w", "x")),
}
ARRANGEMENT = pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))
COLUMNS: dict[str, "pa.Array[Any]"] = {
    "subject": pa.array(["m1", "m1", "m2", "m2"], pa.large_string()),
    "session": pa.array([1, 2, 1, 2], pa.int64()),
    "x": pa.array([10, 20, 30, 40], pa.int64()),
    "w": pa.array([100, 200, 300, 400], pa.int64()),
}
STRINGS = pa.large_list(pa.large_string())
NESTED = pa.large_list(STRINGS)
DEEP = pa.large_list(NESTED)
KEY = "__key"

calls: Counter[str] = Counter()


@pytest.fixture(autouse=True)
def _reset() -> None:
    """Clear the call counts."""
    calls.clear()


@function_pod(output_keys="total")
def add(x: int, w: int) -> int:
    """Return ``x + w``."""
    calls["add"] += 1
    return x + w


@function_pod(output_keys="twice")
def twice(total: int) -> int:
    """Return ``2 * total``."""
    calls["twice"] += 1
    return 2 * total


@function_pod(output_keys="total")
def add_lists(x: list[int], w: list[int]) -> int:
    """Return the sum of every element of both lists."""
    calls["add_lists"] += 1
    return sum(x) + sum(w)


@function_pod(output_keys="total")
def add_nested(x: list[list[int]], w: list[list[int]]) -> int:
    """Return the sum of every element of both lists of lists."""
    calls["add_nested"] += 1
    return sum(sum(v) for v in x) + sum(sum(v) for v in w)


def _source(
    arrangement: str, *, source_id: str = "rows", data_tags: bool = False
) -> ArrowTableSource:
    """The four rows as a source, laid out as ``arrangement`` says."""
    tags, data = ARRANGEMENTS[arrangement]
    names = [*tags, *data]
    schema = pa.schema(pa.field(n, COLUMNS[n].type, nullable=False) for n in names)
    return ArrowTableSource(
        pa.Table.from_arrays([COLUMNS[n] for n in names], schema=schema),
        tag_columns=list(tags),
        tag_only_columns=not data_tags,
        source_id=source_id,
    )


def _with_tokens(
    arrangement: str, tokens: dict[str, tuple[list[Any], pa.DataType]]
) -> ArrowTableStream:
    """The source's rows as a raw stream, each named token column replaced.

    The stream keeps the source's system-tag pair, so the job's slot accepts it.

    Args:
        arrangement: The layout of the rows.
        tokens: For a data column, its token column's values and Arrow type.
    """
    source = _source(arrangement)
    table = source.as_table(columns={"source": True, "system_tags": True})
    for name, (values, kind) in tokens.items():
        column = f"{constants.SOURCE_PREFIX}{name}"
        table = table.set_column(
            table.schema.get_field_index(column),
            pa.field(column, kind, nullable=True),
            pa.array(values, kind),
        )
    return ArrowTableStream(table, tag_columns=list(ARRANGEMENTS[arrangement][0]))


def _stored(database: InMemoryArrowDatabase, function: str) -> pa.Table:
    """The pipeline table (``pdb_v2``) of the function named ``function``."""
    database.flush()
    (path,) = [
        p for p in database.list_sources() if p[-1] == "pdb_v2" and function in p
    ]
    records = database.get_all_records(path, record_id_column=KEY)
    assert records is not None
    return records


def _instance_ids(stored: pa.Table) -> list[bytes]:
    """The stored instance ids, sorted."""
    return sorted(
        cast("list[bytes]", stored.column("__pipeline_instance_id").to_pylist())
    )


def _token_columns(table: pa.Table) -> list[str]:
    """The names of ``table``'s source-info columns, sorted."""
    return sorted(
        n for n in table.column_names if n.startswith(constants.SOURCE_PREFIX)
    )


def _by_instance(stored: pa.Table) -> dict[bytes, dict[str, Any]]:
    """Each stored row's tags and tokens, by its instance id.

    The columns that differ between two runs by design are left out: the result id,
    the record id that covers it, the insertion time and the run id.
    """
    kept = [
        name
        for name in stored.column_names
        if name in COLUMNS or name.startswith(constants.SOURCE_PREFIX)
    ]
    return {
        cast("bytes", row["__pipeline_instance_id"]): {name: row[name] for name in kept}
        for row in stored.to_pylist()
    }


def _check_record_ids(stored: pa.Table) -> None:
    """Every row's record id is the one its own columns give, and is the table's key."""
    for row in stored.to_pylist():
        record = row_ids.record_id(
            HASHER,
            ContentHash.from_prefixed_digest(row["__node_content_hash"]),
            ContentHash.from_prefixed_digest(row["__pipeline_instance_id"]),
            uuid.UUID(bytes=row["__data_id"]),
        ).to_prefixed_digest()
        assert row["__pipeline_record_id"] == record
        assert row[KEY] == record


def _ids_of_given_rows(node: Any) -> tuple[set[bytes], set[bytes]]:
    """The entry ids and the instance ids of the rows ``node`` holds after a run."""
    rows = list(node.iter_data())
    return (
        {row_ids.entry_id(HASHER, t, d).to_prefixed_digest() for t, d in rows},
        {row_ids.instance_id(HASHER, t, d).to_prefixed_digest() for t, d in rows},
    )


def _run_both(build: Any, function: str, given: str) -> dict[str, dict[str, Any]]:
    """Run the job ``build`` makes on each orchestrator, each over its own store.

    Args:
        build: Called with a database; returns the job.
        function: The function whose pipeline table is read.
        given: The label of the node that function reads.

    Returns:
        Per orchestrator: the stored table, its rows by instance id, the token
        columns' types, and the entry and instance ids of the rows the node was given.
    """
    seen: dict[str, dict[str, Any]] = {}
    for name, orchestrator in ORCHESTRATORS.items():
        database = InMemoryArrowDatabase()
        job = build(database)
        _ = job.run(orchestrator=orchestrator())
        stored = _stored(database, function)
        _check_record_ids(stored)
        entries, instances = _ids_of_given_rows(job.nodes[given])
        assert set(stored.column("__pipeline_instance_id").to_pylist()) == instances
        seen[name] = {
            "stored": stored,
            "rows": _by_instance(stored),
            "types": {n: stored.schema.field(n).type for n in _token_columns(stored)},
            "entries": entries,
            "instances": instances,
        }
    return seen


def _assert_same_on_both_routes(seen: dict[str, dict[str, Any]], rows: int) -> None:
    """The two routes stored the same rows, token types and ids."""
    sync, asynchronous = seen["sync"], seen["async"]
    assert sync["stored"].num_rows == asynchronous["stored"].num_rows == rows
    assert sync["types"] == asynchronous["types"]
    assert sync["entries"] == asynchronous["entries"] and len(sync["entries"]) == rows
    assert sync["instances"] == asynchronous["instances"]
    assert len(sync["instances"]) == rows
    assert sync["rows"] == asynchronous["rows"]


# ---------------------------------------------------------------------------
# Token columns made by the operators: one and two aggregation levels
# ---------------------------------------------------------------------------


def _batched(arrangement: str) -> Any:
    return Batch(batch_size=2)(_source(arrangement), label="given")


def _grouped(arrangement: str) -> Any:
    return GroupBy(by=["subject"])(_source(arrangement), label="given")


def _merged(arrangement: str) -> Any:
    return MergeJoin()(
        _source(arrangement, source_id="left"),
        _source(arrangement, source_id="right"),
        label="given",
    )


def _grouped_then_batched(arrangement: str) -> Any:
    return Batch(batch_size=2)(
        GroupBy(by=["subject"])(_source(arrangement)), label="given"
    )


def _merged_then_batched(arrangement: str) -> Any:
    return Batch(batch_size=2)(
        MergeJoin()(
            _source(arrangement, source_id="left"),
            _source(arrangement, source_id="right"),
        ),
        label="given",
    )


AGGREGATIONS: dict[str, tuple[Any, Any, pa.DataType, int]] = {
    "batch": (_batched, add_lists, STRINGS, 2),
    "group_by": (_grouped, add_lists, STRINGS, 2),
    "merge_join": (_merged, add_lists, STRINGS, 4),
    "group_by_then_batch": (_grouped_then_batched, add_nested, NESTED, 1),
    "merge_join_then_batch": (_merged_then_batched, add_nested, NESTED, 2),
}


@ARRANGEMENT
@pytest.mark.parametrize("aggregation", list(AGGREGATIONS))
def test_aggregated_tokens_are_stored_alike_on_both_routes(
    aggregation: str, arrangement: str
) -> None:
    """Aggregated token columns have their stream's type on both routes.

    Over these inputs (plain tags; ``x`` and ``w`` are not keys, and collide under
    ``MergeJoin``) each of ``Batch``, ``GroupBy`` and ``MergeJoin`` gives a list of
    the input's tokens, and two of them a list of lists. A function node stores
    the type its input stream has, not one worked out from the tokens. The stored
    rows, the token types and the entry, instance and record ids are the same on
    both routes.
    """
    make, function, kind, rows = AGGREGATIONS[aggregation]

    def build(database: InMemoryArrowDatabase) -> PipelineJob:
        job = PipelineJob(name="aggregated", database=database)
        with job:
            function.pod(make(arrangement), label="f")
        return job

    seen = _run_both(build, function.pod.label or function.__name__, "given")
    _assert_same_on_both_routes(seen, rows)
    assert seen["sync"]["types"] == {"_source_w": kind, "_source_x": kind}
    assert calls[function.__name__] == 2 * rows


@pytest.mark.parametrize("aggregation", list(AGGREGATIONS))
def test_the_arrangement_of_the_source_changes_no_stored_instance(
    aggregation: str,
) -> None:
    """Tag order and data order of the source change no stored instance id."""
    make, function, _, rows = AGGREGATIONS[aggregation]
    instances = []
    for arrangement in ARRANGEMENTS:

        def build(
            database: InMemoryArrowDatabase, arrangement: str = arrangement
        ) -> PipelineJob:
            job = PipelineJob(name="aggregated", database=database)
            with job:
                function.pod(make(arrangement), label="f")
            return job

        seen = _run_both(build, function.pod.label or function.__name__, "given")
        _assert_same_on_both_routes(seen, rows)
        instances.append(seen["sync"]["instances"])
    assert instances[0] == instances[1]


# ---------------------------------------------------------------------------
# Token columns whose values cannot be typed again from Python values
# ---------------------------------------------------------------------------

# One level (as after ``Batch`` or ``GroupBy``), two levels and three: the values
# hold nulls and empty lists, from which no type can be worked out again (a row
# alone shows fewer list levels than its column has, or none at all).
UNTYPEABLE: dict[str, dict[str, tuple[list[Any], pa.DataType]]] = {
    "one_level": {
        "x": ([[None], [], ["s::0::x", None], ["t"]], STRINGS),
        "w": ([[], ["u", "v"], [None, None], []], STRINGS),
    },
    "two_levels": {
        "x": ([[None], [[]], [["a"], None], [[None, "b"]]], NESTED),
        "w": ([[[]], [["c"]], [None], [[], []]], NESTED),
    },
    "three_levels": {
        "x": ([[None, [[]]], [None, [[]]], [[["t"]]], [[]]], DEEP),
        "w": ([[[["u"]]], [[]], [[None]], [None, [[]]]], DEEP),
    },
    "one_level_beside_strings": {
        "x": ([[None], [], ["s::0::x", None], ["t"]], STRINGS),
    },
}


@ARRANGEMENT
@pytest.mark.parametrize("tokens", list(UNTYPEABLE))
def test_nested_tokens_no_value_can_type_are_stored_alike_on_both_routes(
    tokens: str, arrangement: str
) -> None:
    """Rows with nested token columns pass an operator and reach a function node.

    The job is ``rows -> select -> add``; the slot is bound to a raw stream whose
    token columns are lists (one, two and three levels) holding nulls and empty
    lists. The sync route builds a table from the rows at each edge; it used to type
    every token again from its value there and raise an Arrow type error, while the
    async route sent the rows on (ITL-1491). Both routes now store every row, with
    the stream's token types and the same ids.
    """
    replaced = UNTYPEABLE[tokens]

    def build(database: InMemoryArrowDatabase) -> PipelineJob:
        job = PipelineJob(name="untypeable", database=database)
        with job:
            selected = _source(arrangement).select_data_columns(
                ["w", "x"], label="given"
            )
            add.pod(selected, label="f")
        job.bind(sources={"rows": _with_tokens(arrangement, replaced)})
        return job

    seen = _run_both(build, "add", "given")
    _assert_same_on_both_routes(seen, 4)
    expected = {"_source_w": pa.large_string(), "_source_x": pa.large_string()}
    expected |= {f"_source_{name}": kind for name, (_, kind) in replaced.items()}
    assert seen["sync"]["types"] == expected
    stored = seen["sync"]["stored"].sort_by(
        [("subject", "ascending"), ("session", "ascending")]
    )
    for name, (values, _) in replaced.items():
        assert stored.column(f"_source_{name}").to_pylist() == values
    assert calls["add"] == 8


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("tokens", list(UNTYPEABLE))
def test_the_arrangement_changes_no_instance_of_rows_with_nested_tokens(
    tokens: str, orchestrator: str
) -> None:
    """Tag order and data order change no stored instance id of such rows."""
    instances = []
    for arrangement in ARRANGEMENTS:
        database = InMemoryArrowDatabase()
        job = PipelineJob(name="untypeable", database=database)
        with job:
            selected = _source(arrangement).select_data_columns(
                ["x", "w"], label="given"
            )
            add.pod(selected, label="f")
        job.bind(sources={"rows": _with_tokens(arrangement, UNTYPEABLE[tokens])})
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        instances.append(
            set(_stored(database, "add").column("__pipeline_instance_id").to_pylist())
        )
    assert instances[0] == instances[1] and len(instances[0]) == 4


@pytest.mark.parametrize("first", list(ORCHESTRATORS))
@pytest.mark.parametrize("tokens", list(UNTYPEABLE))
def test_a_second_route_over_the_same_store_finds_every_instance(
    tokens: str, first: str
) -> None:
    """Whichever route ran first, the other finds its records: nothing is added.

    One store, two sessions, one on each orchestrator: the second computes nothing
    and writes no row, because a row has one instance id on both routes.
    """
    database = InMemoryArrowDatabase()
    second = next(name for name in ORCHESTRATORS if name != first)
    stored = []
    for arrangement, orchestrator in zip(ARRANGEMENTS, (first, second)):
        job = PipelineJob(name="untypeable", database=database)
        with job:
            selected = _source(arrangement).select_data_columns(
                ["x", "w"], label="given"
            )
            add.pod(selected, label="f")
        job.bind(sources={"rows": _with_tokens(arrangement, UNTYPEABLE[tokens])})
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        table = _stored(database, "add")
        assert table.num_rows == 4, orchestrator
        stored.append(_instance_ids(table))
    assert stored[0] == stored[1]
    assert calls["add"] == 4


# ---------------------------------------------------------------------------
# A real pipeline case: an aggregation over a token column that holds nulls
# ---------------------------------------------------------------------------

# One-level token columns that are null in some rows. ``Batch`` and ``GroupBy``
# declare two levels over them, with values such as ``[None]`` and ``[None, None]``,
# which show one level only: typed from its value, such a token lost a level.
NULL_LISTS: dict[str, tuple[list[Any], pa.DataType]] = {
    "x": ([None, ["a"], None, None], STRINGS),
    "w": ([["b"], None, ["c", "d"], []], STRINGS),
}
OVER_NULL_LISTS: dict[str, tuple[Any, int]] = {
    "batch_1": (lambda stream: Batch(batch_size=1)(stream, label="given"), 4),
    "batch_2": (lambda stream: Batch(batch_size=2)(stream, label="given"), 2),
    "group_by": (lambda stream: GroupBy(by=["subject"])(stream, label="given"), 2),
}


@function_pod(output_keys="count")
def count_members(x: list[int], w: list[int]) -> int:
    """Return how many members the row gathers."""
    calls["count_members"] += 1
    return len(x) + len(w)


def _null_list_job(
    database: InMemoryArrowDatabase, aggregation: str, arrangement: str
) -> PipelineJob:
    """``rows -> Batch or GroupBy -> count_members``, the slot bound to ``NULL_LISTS``."""
    job = PipelineJob(name="null_lists", database=database)
    with job:
        count_members.pod(
            OVER_NULL_LISTS[aggregation][0](_source(arrangement)), label="f"
        )
    job.bind(sources={"rows": _with_tokens(arrangement, NULL_LISTS)})
    return job


@ARRANGEMENT
@pytest.mark.parametrize("aggregation", list(OVER_NULL_LISTS))
def test_an_aggregation_over_null_list_tokens_keeps_two_levels_on_both_routes(
    aggregation: str, arrangement: str
) -> None:
    """``Batch`` and ``GroupBy`` over a list token column with nulls keep both levels.

    The aggregated token column is a list of token lists, and some of its values
    are ``[None]`` or ``[None, None]``. The function node after it stores the
    declared two levels on the sync and on the async route, with the same rows
    and ids (ITL-1491: the sync edge typed ``[None]`` as one level).
    """
    rows = OVER_NULL_LISTS[aggregation][1]
    seen = _run_both(
        lambda database: _null_list_job(database, aggregation, arrangement),
        "count_members",
        "given",
    )
    _assert_same_on_both_routes(seen, rows)
    assert seen["sync"]["types"] == {"_source_w": NESTED, "_source_x": NESTED}
    tokens = seen["sync"]["stored"].column("_source_x").to_pylist()
    assert any(token is not None and None in token for token in tokens)
    assert calls["count_members"] == 2 * rows


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("aggregation", list(OVER_NULL_LISTS))
def test_held_rows_of_such_an_aggregation_reach_the_function_again(
    aggregation: str, orchestrator: str
) -> None:
    """The aggregation's held rows are passed to the function node with their types.

    After a run the function node is cleared, so the next run holds the aggregation
    node (it is current) and passes its rows on to the function node, which finds
    every instance stored: no row is added and nothing is computed. The sync route
    builds the held rows into one table at that edge, where ``[None]`` used to be
    typed as one level.
    """
    rows = OVER_NULL_LISTS[aggregation][1]
    database = InMemoryArrowDatabase()
    job = _null_list_job(database, aggregation, "as_declared")
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    first = _stored(database, "count_members")
    job.nodes["f"].clear_cache()
    summary = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert summary.nodes["f"].reused == rows and summary.nodes["f"].computed == 0
    again = _stored(database, "count_members")
    assert again.num_rows == first.num_rows == rows
    assert _instance_ids(again) == _instance_ids(first)
    assert again.schema.field("_source_x").type == NESTED
    assert again.schema.field("_source_w").type == NESTED
    assert len(list(job.nodes["f"].iter_data())) == rows
    assert calls["count_members"] == rows


# ---------------------------------------------------------------------------
# Data-tags that fall through two functions keep their tokens, type and value
# ---------------------------------------------------------------------------

TAG_ORDERS = [("subject", "session"), ("session", "subject")]
FIXED = pa.list_(pa.large_string(), 2)
# The token column of each data-tag: a fixed-size list (a type no value gives), a
# nested list holding a null and an empty list, and a string column with a null.
DATA_TAG_TOKENS: dict[str, tuple[list[Any], pa.DataType]] = {
    "subject": ([["p", "q"], ["r", "s"], ["t", "u"], ["v", "w"]], FIXED),
    "session": ([[None, ["a"]], [[]], [["b"], None], [[None]]], NESTED),
}


def _data_tag_stream(tags: tuple[str, ...]) -> ArrowTableStream:
    """The four rows as a raw stream whose two tags are data-tags with typed tokens."""
    arrangement = "as_declared" if tags == TAG_ORDERS[0] else "reversed"
    source = _source(arrangement, data_tags=True)
    table = source.as_table(columns={"source": True, "system_tags": True})
    for name, (values, kind) in DATA_TAG_TOKENS.items():
        column = f"{constants.SOURCE_PREFIX}{name}"
        table = table.set_column(
            table.schema.get_field_index(column),
            pa.field(column, kind, nullable=True),
            pa.array(values, kind),
        )
    return ArrowTableStream(table, tag_columns=list(tags), data_tags=list(tags))


def _data_tag_job(
    database: InMemoryArrowDatabase, tags: tuple[str, ...]
) -> PipelineJob:
    """``rows -> add -> twice`` over a source whose two tags are data-tags."""
    arrangement = "as_declared" if tags == TAG_ORDERS[0] else "reversed"
    job = PipelineJob(name="data_tags", database=database)
    with job:
        first = add.pod(_source(arrangement, data_tags=True), label="given")
        twice.pod(first, label="f")
    job.bind(sources={"rows": _data_tag_stream(tags)})
    return job


@pytest.mark.parametrize("tags", TAG_ORDERS)
def test_data_tags_fall_through_two_functions_with_their_tokens_unchanged(
    tags: tuple[str, ...],
) -> None:
    """After each function a data-tag's token is the stream's, type and value.

    Two data-tags fall through ``add`` and then ``twice``. The token column of each,
    in the rows after the first function, in the rows after the second and in the
    second function's stored rows, equals the stream's: a fixed-size list stays a
    fixed-size list and a nested list its type, on both routes. A column a function
    returns has the function's own string token.
    """
    before = {
        (cast("str", data["subject"]), cast("int", data["session"])): data
        for _, data in _data_tag_stream(tags).iter_data()
    }
    seen = _run_both(lambda database: _data_tag_job(database, tags), "twice", "given")
    # Each route ran over its own store, so ``total``'s token holds another result
    # id on each and the instance ids differ (one store: the test below). The
    # entries, the token types and the data-tags' tokens are the same.
    assert seen["sync"]["entries"] == seen["async"]["entries"]
    assert len(seen["sync"]["entries"]) == 4
    assert seen["sync"]["types"] == seen["async"]["types"]
    fallen = [f"{constants.SOURCE_PREFIX}{name}" for name in DATA_TAG_TOKENS]
    for route in seen.values():
        assert route["stored"].num_rows == 4
        ordered = route["stored"].sort_by(
            [("subject", "ascending"), ("session", "ascending")]
        )
        for column in fallen:
            name = column.removeprefix(constants.SOURCE_PREFIX)
            assert ordered.column(column).to_pylist() == DATA_TAG_TOKENS[name][0]
    assert seen["sync"]["types"] == {
        "_source_session": NESTED,
        "_source_subject": FIXED,
        "_source_total": pa.large_string(),
    }
    for orchestrator in ORCHESTRATORS.values():
        job = _data_tag_job(InMemoryArrowDatabase(), tags)
        _ = job.run(orchestrator=orchestrator())
        for label, returned in (("given", "total"), ("f", "twice")):
            rows = list(job.nodes[label].iter_data())
            assert len(rows) == 4
            for _, data in rows:
                origin = before[(data["subject"], data["session"])].source_info_table()
                after = data.source_info_table()
                for name in ("subject", "session"):
                    column = f"{constants.SOURCE_PREFIX}{name}"
                    assert after.schema.field(column).type == DATA_TAG_TOKENS[name][1]
                    assert after.column(column).equals(origin.column(column))
                own = after.schema.field(f"{constants.SOURCE_PREFIX}{returned}")
                assert own.type == pa.large_string()
                assert f"::{returned}" in cast("str", data.source_info()[returned])


@pytest.mark.parametrize(
    "routes",
    [("sync", "async"), ("async", "sync"), ("sync", "sync", "async", "async")],
)
def test_data_tags_have_one_instance_per_row_whichever_route_ran_first(
    routes: tuple[str, ...],
) -> None:
    """Sessions on either route, in both tag orders, over one store: no row is added."""
    database = InMemoryArrowDatabase()
    stored = []
    for index, route in enumerate(routes):
        job = _data_tag_job(database, TAG_ORDERS[index % 2])
        _ = job.run(orchestrator=ORCHESTRATORS[route]())
        table = _stored(database, "twice")
        assert table.num_rows == 4, (index, route)
        assert table.schema.field("_source_subject").type == FIXED
        assert table.schema.field("_source_session").type == NESTED
        stored.append(_instance_ids(table))
    assert all(ids == stored[0] for ids in stored) and len(set(stored[0])) == 4
    assert calls["add"] == 4 and calls["twice"] == 4
