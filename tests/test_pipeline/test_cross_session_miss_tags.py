"""A row recomputed in a later session keeps its stored tag (ITL-1298).

A function job node whose results are ephemeral stores its pipeline entries in the
pipeline database and its results in an ephemeral database. In a later session (a new
job over the same database, with a new, empty ephemeral database) every such result
is gone: a cross-session miss. The node loads the stored entry and holds an
``EmptyData`` placeholder for it, with the row's tag. That tag used to be rebuilt from
the stored row's Python values with no schema, so its types were inferred again: an
``int8`` tag became ``int64`` (another tag hash), and a struct tag could not be read at
all. It is now built from the stored one-row Arrow table, with the stored types.

The placeholder's tag is visible only in the node's loaded entries: in every public
read the node computes the row again first (a run), or leaves the placeholder out
(``head``, a cache-only send, a load-only run). So the tests read the loaded entries
through ``_load_cached_entries``, the one place that shows them, and check through the
public interface that nothing a user sees or stores changes: the run counts, the
presented rows (tag types, tag hash, record id, values), and the stored pipeline rows
(each recomputed row on its own base entry, as the next recomputation).

The source has tags of several types (``int8``, ``float32``, ``large_string``, a struct,
a ``large_list`` and the logical ``Path``), two or more at a time and in more than one
tag order, as plain tags and as data-tags; one ``int64`` data column ``x``. The graph is
``inc`` (node ``one``) then ``dbl`` (node ``two``): both nodes ephemeral, or only
``one``. Every case runs on the sync and the async orchestrator.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.core.datagrams import EmptyData
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import TagProtocol
from orcapod.system_constants import constants
from orcapod.types import NodeConfig

CTX = get_default_context()
PATH_TYPE = cast(
    pa.BaseExtensionType, CTX.type_converter.python_type_to_arrow_type(Path)
)

TAG_COLUMNS: dict[str, "pa.Array[Any]"] = {
    "i8": pa.array([1, 2], pa.int8()),
    "f32": pa.array([0.5, 1.5], pa.float32()),
    "s": pa.array(["é", "b"], pa.large_string()),
    "st": pa.array(
        [{"a": 1, "b": "u"}, {"a": 2, "b": "v"}],
        pa.struct([("a", pa.int64()), ("b", pa.large_string())]),
    ),
    "l": pa.array([[1, 2], [3]], pa.large_list(pa.int64())),
    "p": pa.ExtensionArray.from_storage(
        PATH_TYPE, pa.array(["/a", "/b"], pa.large_string())
    ),
}
# Two or more tags at a time, each set in two tag orders.
TAG_SETS = [
    ("i8", "f32", "s"),
    ("s", "f32", "i8"),
    ("st", "l", "p", "i8"),
    ("i8", "p", "l", "st"),
]
PLAIN, DATA_TAGS = "plain", "data_tags"
BOTH_EPHEMERAL, ONE_EPHEMERAL = "both_ephemeral", "one_ephemeral"
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


@function_pod(output_keys="y")
def inc(x: int) -> int:
    return x + 1


@function_pod(output_keys="z")
def dbl(y: int) -> int:
    return 2 * y


def _source(tags: tuple[str, ...], role: str) -> ArrowTableSource:
    arrays = {**{name: TAG_COLUMNS[name] for name in tags}, "x": pa.array([10, 20])}
    schema = pa.schema(pa.field(n, a.type, nullable=False) for n, a in arrays.items())
    return ArrowTableSource(
        pa.Table.from_arrays(list(arrays.values()), schema=schema),
        tag_columns=list(tags),
        tag_only_columns=True if role == PLAIN else [],
        source_id="s",
    )


def _job(
    database: Any,
    ephemeral: InMemoryArrowDatabase,
    tags: tuple[str, ...],
    role: str,
    config: str,
) -> PipelineJob:
    job = PipelineJob(name="itl1298", database=database)
    with job:
        dbl.pod(inc.pod(_source(tags, role), label="one"), label="two")
    job.set_ephemeral_database(ephemeral)
    if config == BOTH_EPHEMERAL:
        job.apply_node_config(NodeConfig(is_result_ephemeral=True))
    else:
        job.nodes["one"].node_config = NodeConfig(is_result_ephemeral=True)  # pyright: ignore[reportAttributeAccessIssue]
    return job


def _record_id(tag: TagProtocol) -> str:
    return next(
        str(value)
        for name, value in tag.system_tags().items()
        if name.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
    )


def _type_text(arrow_type: pa.DataType) -> str:
    """An Arrow type as text, a list's child field name left out.

    The Delta store reads a list's child field back as ``element`` where it was written
    as ``item`` (the store's own type changes, ITL-890 and ITL-999); the name enters no
    hash, and the tag hash is compared too.
    """
    if pa.types.is_large_list(arrow_type):
        return f"large_list<{_type_text(arrow_type.value_type)}>"
    return str(arrow_type)


def _tag_facts(tag: TagProtocol) -> tuple[str, dict[str, str], str, str]:
    """(record id, tag Arrow types, tag hash, tag values): the struct tag must read."""
    return (
        _record_id(tag),
        {f.name: _type_text(f.type) for f in tag.arrow_schema()},
        tag.content_hash().to_string(),
        repr(sorted(tag.as_dict().items(), key=lambda item: item[0])),
    )


def _presented(job: PipelineJob, label: str) -> dict[str, tuple[Any, ...]]:
    return {
        _record_id(tag): (*_tag_facts(tag)[1:], repr(sorted(data.as_dict().items())))
        for tag, data in job.nodes[label].iter_data()
    }


def _stored(database: Any, function: str) -> list[tuple[Any, ...]]:
    """The stored pipeline rows of ``function``: (base entry, recomputation index)."""
    database.flush()
    [path] = [
        p
        for p in database.list_table_paths()
        if p[1] == function and p[-1].startswith("pdb")
    ]
    table = database.get_all_records(path)
    return sorted(
        zip(
            table.column("__pipeline_base_entry_id").to_pylist(),
            table.column("__pipeline_recomputation_index").to_pylist(),
        )
    )


@pytest.mark.parametrize("store", ["memory", "delta"])
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("config", [BOTH_EPHEMERAL, ONE_EPHEMERAL])
@pytest.mark.parametrize("role", [PLAIN, DATA_TAGS])
@pytest.mark.parametrize("tags", TAG_SETS, ids=lambda t: "-".join(t))
def test_a_cross_session_miss_keeps_its_stored_tag(
    tags: tuple[str, ...],
    role: str,
    config: str,
    orchestrator: str,
    store: str,
    tmp_path: Path,
) -> None:
    """The pipeline database is in memory or a Delta store; the ephemeral one is in
    memory, new in each session."""
    database: Any = (
        InMemoryArrowDatabase()
        if store == "memory"
        else DeltaTableDatabase(base_path=tmp_path / "store")
    )
    ephemeral_nodes = ["one", "two"] if config == BOTH_EPHEMERAL else ["one"]

    # Session 1: every row computed and stored.
    first = _job(database, InMemoryArrowDatabase(), tags, role, config)
    summary = first.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert {
        label: (summary.nodes[label].computed, summary.nodes[label].reused)
        for label in ("one", "two")
    } == {"one": (2, 0), "two": (2, 0)}
    presented = {label: _presented(first, label) for label in ("one", "two")}
    stored = {function: _stored(database, function) for function in ("inc", "dbl")}
    assert sorted(presented["one"]) == ["row_0", "row_1"]
    for label in ("one", "two"):
        for types, *_ in presented[label].values():
            assert types == {name: _type_text(TAG_COLUMNS[name].type) for name in tags}

    # Session 2: a new job over the same database, a new empty ephemeral database.
    second = _job(database, InMemoryArrowDatabase(), tags, role, config)
    for label in ephemeral_nodes:
        # Only the node's loaded entries show a miss row's tag (see the module).
        loaded = second.nodes[label]._load_cached_entries()  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]  # the test is about it
        assert len(loaded) == 2
        for tag, data in cast(dict[bytes, tuple[TagProtocol, Any]], loaded).values():
            assert isinstance(data, EmptyData)
            record_id, types, tag_hash, values = _tag_facts(tag)
            want_types, want_hash, want_values, _ = presented[label][record_id]
            assert types == want_types, (label, record_id)
            assert tag_hash == want_hash, (label, record_id)
            assert values == want_values, (label, record_id)

    summary = second.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert {
        label: (summary.nodes[label].computed, summary.nodes[label].reused)
        for label in ("one", "two")
    } == {
        "one": (2, 0),
        "two": (2, 0) if config == BOTH_EPHEMERAL else (0, 2),
    }
    for label in ("one", "two"):
        assert _presented(second, label) == presented[label], label
    # Each recomputed row lands on its own base entry, as the next recomputation; a
    # persistent node's rows are reused and nothing new is stored.
    for function, label in (("inc", "one"), ("dbl", "two")):
        now = _stored(database, function)
        if label in ephemeral_nodes:
            assert now == sorted(
                [*stored[function], *[(entry, 1) for entry, _ in stored[function]]]
            )
        else:
            assert now == stored[function]
