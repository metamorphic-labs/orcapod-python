"""A function node's entry ids and input data hashes have one definition (ITL-1361).

``FunctionJobNode._input_entries`` takes each input row's base entry id from slices of
the input stream's Arrow table (``_base_entry_id_of_slices``) where the slices are
provably the rows' own tables, and from the rows (``compute_base_entry_id``) otherwise.
Both go through the same definitions: the preimage's last column is
``_with_input_data_hash``, and the input data hash is ``data_table_content_hash``, the
content hash a data datagram has. These tests check, byte for byte, that the slice-built
ids and input data hashes equal the row-built ones over the several-items family (tag
order, table layout, tag roles), nullable columns, logical and numpy values, list-valued
columns after ``Batch`` and ``GroupBy``, nested source info, and the inputs whose rows
type their columns otherwise (which take the row path); and that a store written by one
path is found by the other, with the same record ids and no call of the function.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.data_tags import stream_data_tags
from orcapod.core.nodes import function_node as function_node_module
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import Batch, GroupBy
from orcapod.sources import DictSource
from tests.test_core.operators.several_items import MAIN, arrangements, make_source


@function_pod(output_keys="c")
def _constant(d1: int) -> int:
    return d1


def _node() -> FunctionJobNode:
    """A function node; only its data context's hashers are used by the ids."""
    return FunctionJobNode(
        _constant.pod,  # pyright: ignore[reportArgumentType]  # a function pod; the checker does not match it to FunctionPodProtocol
        make_source(MAIN).select_data_columns(["d1"]),
        pipeline_database=InMemoryArrowDatabase(),
    )


def _forms(stream: Any) -> dict[str, ArrowTableStream]:
    """``stream`` as a job hands it on (rebuilt from its rows) and as a raw stream."""
    rows = list(stream.iter_data())
    handed_on: Any = StaticOutputOperatorPod._materialize_upstream(rows, stream)  # pyright: ignore[reportPrivateUsage]  # the stream a job hands a node
    raw = ArrowTableStream(
        stream.as_table(columns={"system_tags": True, "source": True}),
        tag_columns=stream.keys()[0],
        data_tags=stream_data_tags(stream),
    )
    return {"handed_on": handed_on, "raw": raw}


def _outcome(compute: Any) -> Any:
    """``compute()``, or the type and text of the error it raises."""
    try:
        return compute()
    except Exception as error:  # noqa: BLE001  # the outcome, error or value, is compared
        return (type(error), str(error))


def _assert_one_definition(stream: Any) -> None:
    """Slice-built ids and input data hashes equal the row-built ones, byte for byte
    (or both paths raise the same error)."""
    node = _node()
    sliced = _outcome(
        lambda: [entry_id for _, _, entry_id in node._input_entries(stream)]  # pyright: ignore[reportPrivateUsage]  # under test
    )
    by_rows = _outcome(
        lambda: [
            node.compute_base_entry_id(tag, data) for tag, data in stream.iter_data()
        ]
    )
    assert sliced == by_rows
    table = stream.as_table(columns={"system_tags": True})
    takes_slices = function_node_module._reads_as_a_table_stream(  # pyright: ignore[reportPrivateUsage]  # the slice path's precondition
        stream
    ) and function_node_module._slices_are_rows(table)  # pyright: ignore[reportPrivateUsage]  # the slice path's precondition
    if isinstance(sliced, list) and takes_slices:
        _, data_keys = stream.keys()
        data_side = table.select(list(data_keys))
        for index, (_, data) in enumerate(stream.iter_data()):
            hashed = function_node_module.data_table_content_hash(
                data_side.slice(index, 1), stream.data_context
            )
            assert (
                hashed.to_prefixed_digest() == data.content_hash().to_prefixed_digest()
            )


ARRANGEMENTS = arrangements(MAIN)


@pytest.mark.parametrize("form", ["handed_on", "raw"])
@pytest.mark.parametrize("arrangement", ARRANGEMENTS, ids=[a.id for a in ARRANGEMENTS])
def test_the_several_items_family(arrangement: Any, form: str) -> None:
    """Every tag order, table layout and tag role of the shared family."""
    _assert_one_definition(_forms(make_source(MAIN, arrangement))[form])


@pytest.mark.parametrize("form", ["handed_on", "raw"])
@pytest.mark.parametrize(
    "arrangement", ARRANGEMENTS[::4], ids=[a.id for a in ARRANGEMENTS[::4]]
)
@pytest.mark.parametrize("aggregate", ["batch", "group_by", "batch_of_batch"])
def test_list_columns_and_nested_source_info(
    arrangement: Any, aggregate: str, form: str
) -> None:
    """List-valued data and tags after ``Batch`` and ``GroupBy``; source info nested
    one level deeper after a second aggregation."""
    source = make_source(MAIN, arrangement)
    if aggregate == "batch":
        stream: Any = Batch(batch_size=2)(source)
    elif aggregate == "group_by":
        stream = GroupBy(by=[arrangement.tag_order[0]])(source)
    else:
        stream = Batch(batch_size=2)(Batch(batch_size=2)(source))
    _assert_one_definition(_forms(stream)[form])


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
@pytest.mark.parametrize("form", ["handed_on", "raw"])
@pytest.mark.parametrize("roles", ["plain", "data_tag"])
def test_logical_numpy_and_nullable_values(form: str, roles: str) -> None:
    """A path, a numpy array, a nullable integer and a nullable string, with two tags."""
    rows = [
        {
            "a": 1,
            "b": "x",
            "p": Path("one"),
            "n": np.array([1.0, -0.0]),
            "i": 1,
            "s": None,
        },
        {
            "a": 2,
            "b": "y",
            "p": Path("two"),
            "n": np.array([[1, 2], [3, 4]]),
            "i": None,
            "s": "s",
        },
    ]
    source = DictSource(
        rows,
        tag_columns=["a", "b"],
        tag_only_columns=True if roles == "plain" else ["a"],
        source_id="values",
    )
    _assert_one_definition(_forms(source)[form])


def _with_system_tag(column: Any, data: Any = None) -> ArrowTableStream:
    table = pa.table(
        {"k": [1, 2], "r": column, "d1": data if data is not None else [10, 20]}
    )
    return ArrowTableStream(table, tag_columns=["k"], system_tag_columns=["r"])


class _Filtering(ArrowTableStream):
    def iter_data(self) -> Any:
        return (row for row in super().iter_data() if cast(int, row[1]["d1"]) > 10)


@pytest.mark.parametrize(
    "stream",
    [
        pytest.param(
            lambda: _with_system_tag(pa.array([1, 2], pa.int16())),
            id="int16_system_tag",
        ),
        pytest.param(
            lambda: _with_system_tag(pa.array(["a", None], pa.large_string())),
            id="nullable_system_tag",
        ),
        pytest.param(
            lambda: _with_system_tag(
                pa.array(["a", "b"]), pa.array([None, None], pa.null())
            ),
            id="null_data_column",
        ),
        pytest.param(
            lambda: _Filtering(
                pa.table({"k": [1, 2], "d1": [10, 20]}), tag_columns=["k"]
            ),
            id="filtering_subclass",
        ),
    ],
)
def test_inputs_whose_rows_type_columns_otherwise_take_the_row_path(
    stream: Any,
) -> None:
    """Where the slices are not the rows' own tables (a row rebuilds its system tags
    from their values, a datagram maps ``null`` to ``large_string``, a subclass reads
    its rows its own way), the ids are taken from the rows, as on ``main``."""
    _assert_one_definition(stream())


# ── a store written by one path is found by the other ─────────────────────────

CALLS: list[int] = []


@function_pod(output_keys="doubled")
def _double(d1: int) -> int:
    CALLS.append(d1)
    return 2 * d1


def _run(database: InMemoryArrowDatabase, *, slices: bool, monkeypatch: Any) -> Any:
    """Run a job of one function node over the family's source on ``database``."""
    monkeypatch.setattr(
        function_node_module,
        "_slices_are_rows",
        function_node_module._slices_are_rows if slices else (lambda table: False),  # pyright: ignore[reportPrivateUsage]  # the path under test
    )
    job = PipelineJob(name="paths", database=database)
    with job:
        _double.pod(make_source(MAIN).select_data_columns(["d1"]), label="doubled")
    job.run()
    node: Any = job.nodes["doubled"]
    joined = node._fetch_joined_records()  # pyright: ignore[reportPrivateUsage]  # the stored entry ids
    assert joined is not None
    entry_ids = joined.table["__pipeline_base_entry_id"].to_pylist()
    return sorted(entry_ids), node.as_table()


@pytest.mark.parametrize(
    ("first", "second"),
    [(False, True), (True, False)],
    ids=["rows_then_slices", "slices_then_rows"],
)
def test_a_store_written_by_one_path_is_found_by_the_other(
    first: bool, second: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same stored entry ids; the second run computes nothing and gives the same rows."""
    database = InMemoryArrowDatabase()
    CALLS.clear()
    written, first_table = _run(database, slices=first, monkeypatch=monkeypatch)
    assert CALLS
    CALLS.clear()
    found, second_table = _run(database, slices=second, monkeypatch=monkeypatch)
    assert CALLS == []
    assert found == written
    assert second_table.equals(first_table)


# ── what observers receive ─────────────────────────────────────────────────────


def _observed_rows(observer: Any) -> None:
    """Run a function job twice (computed, then from the store) with ``observer``."""
    database = InMemoryArrowDatabase()
    for _ in range(2):
        job = PipelineJob(name="observed", database=database)
        with job:
            _double.pod(make_source(MAIN).select_data_columns(["d1"]), label="doubled")
        job.run(observer=observer)


def test_an_observer_that_may_read_rows_receives_the_rows_themselves() -> None:
    """Any observer but the library's row-blind ones (here a ``NoOpObserver``
    subclass) receives the rows themselves, as on ``main``: concrete ``Tag`` and
    ``Data``, on computed rows and on stored results."""
    from orcapod.core.datagrams import Data, Tag
    from orcapod.pipeline.observer import NoOpObserver
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol

    seen: list[tuple[Any, Any]] = []

    class Watch(NoOpObserver):
        def contextualize(self, *identity_path: str) -> Any:
            return self

        def on_data_start(self, node_label: str, tag: Any, data: Any) -> None:
            seen.append((tag, data))

    _observed_rows(Watch())
    assert len(seen) == 12
    for tag, data in seen:
        assert type(tag) is Tag and type(data) is Data
        assert isinstance(tag, TagProtocol) and isinstance(data, DataProtocol)


def test_row_views_are_tags_and_data_for_every_public_check() -> None:
    """The views a row-blind observer would receive pass the runtime protocol checks
    (every member declared on the class), compare, hash and print as their datagram,
    and a pickle or copy of one is the datagram itself."""
    import copy
    import pickle

    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol

    stream: Any = _forms(make_source(MAIN).select_data_columns(["d1"]))["handed_on"]
    node = _node()
    entries = node._input_entries(stream, row_views=True)  # pyright: ignore[reportPrivateUsage]  # the views
    rows = list(stream.iter_data())
    for (tag, data, _), (real_tag, real_data) in zip(entries, rows, strict=True):
        assert type(tag).__name__ == "_TagView" and type(data).__name__ == "_DataView"
        assert isinstance(tag, TagProtocol) and isinstance(data, DataProtocol)
        assert not isinstance(tag, DataProtocol) or isinstance(real_tag, DataProtocol)
        assert tag == real_tag and data == real_data
        assert hash(tag) == hash(real_tag)
        assert repr(data) == repr(real_data)
        assert tag.keys() == real_tag.keys()
        assert data.data_context_key == real_data.data_context_key
        # Pickling does what pickling the datagram does (a datagram holds a lock).
        assert _outcome(lambda: pickle.dumps(data)) == _outcome(
            lambda: pickle.dumps(real_data)
        )
        assert type(copy.copy(tag)) is type(real_tag)


def test_only_the_library_row_blind_observers_get_views() -> None:
    """``reads_no_rows``: exactly the classes that declare it, never a subclass; a
    composite only when every observer in it reads none."""
    from orcapod.pipeline.composite_observer import CompositeObserver
    from orcapod.pipeline.observer import NoOpObserver
    from orcapod.pipeline.run_summary import RunCountingObserver
    from orcapod.utils.observer_rows import reads_no_rows

    class Watch(NoOpObserver):
        pass

    assert reads_no_rows(NoOpObserver())
    assert reads_no_rows(RunCountingObserver())
    assert reads_no_rows(CompositeObserver(NoOpObserver(), RunCountingObserver()))
    assert not reads_no_rows(Watch())
    assert not reads_no_rows(CompositeObserver(Watch(), RunCountingObserver()))
    assert not reads_no_rows(object())


# ── a side-effect node's pass-through ─────────────────────────────────────────


def _delivered(x: int | None, ctx: Any) -> None:
    pass


def _side_effect_edge(stream: ArrowTableStream) -> Any:
    from orcapod.core.operators.static_output_pod import at_edge
    from orcapod.side_effects import (
        SideEffectJobNode,
        SideEffectPodConfig,
        side_effect_pod,
    )

    base = ArrowTableStream(pa.table({"k": ["a", "b"], "x": [1, 2]}), tag_columns=["k"])
    pod: Any = side_effect_pod(
        _delivered, config=SideEffectPodConfig(track_completion=False)
    )
    node = SideEffectJobNode(pod, base)
    return at_edge(node.execute(stream, load_only=True))


def test_a_pass_through_whose_rows_retype_system_tags_is_main_s_row_build() -> None:
    """An ``int32`` system tag: the edge has ``main``'s row-built ``int64``; a string
    system tag holding a null: the edge raises ``main``'s ``ArrowInvalid``."""
    narrow = ArrowTableStream(
        pa.table({"k": ["a", "b"], "x": [1, 2], "e": pa.array([9, 10], pa.int32())}),
        tag_columns=["k"],
        system_tag_columns=["e"],
    )
    edge = _side_effect_edge(narrow).as_table(columns={"system_tags": True})
    (name,) = [n for n in edge.column_names if n.startswith("_tag")]
    assert edge.schema.field(name).type == pa.int64()

    nullable = ArrowTableStream(
        pa.table(
            {
                "k": ["a", "b"],
                "x": [1, 2],
                "e": pa.array(["a", None], pa.large_string()),
            }
        ),
        tag_columns=["k"],
        system_tag_columns=["e"],
    )
    with pytest.raises(pa.ArrowInvalid):
        _side_effect_edge(nullable)
