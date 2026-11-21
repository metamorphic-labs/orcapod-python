"""A row recomputed in a later session keeps its stored tag (ITL-1298).

A function job node whose results are ephemeral stores its pipeline entries in the
pipeline database and its results in an ephemeral database. In a later session (a new
job over the same database, with a new, empty ephemeral database) every such result
is gone: a cross-session miss. The node loads the stored entry and holds an
``EmptyData`` placeholder for it, with the row's tag. That tag used to be rebuilt from
the stored row's Python values with no schema, so its types were inferred again: an
``int8`` tag became ``int64`` (another tag hash), and a struct tag could not be read at
all. It is now built from the stored one-row Arrow table, with the stored types and
without the storage-only data-tag mark.

The node's cache facade, ``get_cached_results``, returns such a row as
``(tag, EmptyData)`` without computing it: that is where the placeholder's tag shows,
and where the tests read it, beside row 0's cached result in a mixed load. A run
computes the row again first, and ``head``, a cache-only send and a load-only run leave
the placeholder out. So the tests check that the tag ``get_cached_results`` returns has
the first session's fields (types, nullability, metadata, system tags), hash and values,
and that the run counts, the presented rows and the stored pipeline rows (each
recomputed row on its own base entry, as the next recomputation) are unchanged.

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


def _source(tags: tuple[str, ...], role: str, rows: int = 2) -> ArrowTableSource:
    arrays = {
        **{name: TAG_COLUMNS[name].slice(0, rows) for name in tags},
        "x": pa.array([10, 20][:rows]),
    }
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
    rows: int = 2,
) -> PipelineJob:
    job = PipelineJob(name="itl1298", database=database)
    with job:
        dbl.pod(inc.pod(_source(tags, role, rows), label="one"), label="two")
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


def _fields(tag: TagProtocol) -> list[tuple[Any, ...]]:
    """Every field of the tag (system tags included): name, type, nullability and
    metadata (a list's child field name left out, see ``_type_text``). Empty metadata
    is no metadata, as Arrow's own ``equals(..., check_metadata=True)`` holds it (the
    Delta store reads a logical field back with ``{}``)."""
    return [
        (f.name, _type_text(f.type), f.nullable, f.metadata or None)
        for f in tag.arrow_schema(columns={"system_tags": True})
    ]


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


ALL_MISSING, MIXED_LOAD = "all_missing", "mixed"


@pytest.mark.parametrize("scenario", [ALL_MISSING, MIXED_LOAD])
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
    scenario: str,
    tmp_path: Path,
) -> None:
    """The pipeline database is in memory or a Delta store; the ephemeral one is in
    memory. In session 2 every result is gone (``all_missing``), or row 0's result is
    there and row 1's is gone (``mixed``: a session in between ran row 0 alone over
    session 2's ephemeral database)."""
    database: Any = (
        InMemoryArrowDatabase()
        if store == "memory"
        else DeltaTableDatabase(base_path=tmp_path / "store")
    )
    ephemeral_nodes = ["one", "two"] if config == BOTH_EPHEMERAL else ["one"]
    run = ORCHESTRATORS[orchestrator]

    # Session 1: every row computed and stored.
    first = _job(database, InMemoryArrowDatabase(), tags, role, config)
    summary = first.run(orchestrator=run())
    assert {
        label: (summary.nodes[label].computed, summary.nodes[label].reused)
        for label in ("one", "two")
    } == {"one": (2, 0), "two": (2, 0)}
    presented = {label: _presented(first, label) for label in ("one", "two")}
    first_fields = {
        label: {
            _record_id(tag): _fields(tag) for tag, _ in first.nodes[label].iter_data()
        }
        for label in ("one", "two")
    }
    stored = {function: _stored(database, function) for function in ("inc", "dbl")}
    assert sorted(presented["one"]) == ["row_0", "row_1"]
    for label in ("one", "two"):
        for types, *_ in presented[label].values():
            assert types == {name: _type_text(TAG_COLUMNS[name].type) for name in tags}

    ephemeral = InMemoryArrowDatabase()
    if scenario == MIXED_LOAD:
        # Row 0 alone, over session 2's ephemeral database: its results are there.
        _ = _job(database, ephemeral, tags, role, config, rows=1).run(
            orchestrator=run()
        )
    before = {function: _stored(database, function) for function in ("inc", "dbl")}

    # Session 2: a new job over the same database.
    second = _job(database, ephemeral, tags, role, config)
    for label, function in (("one", "inc"), ("two", "dbl")):
        if label not in ephemeral_nodes:
            continue
        # The public cache facade returns a miss row as (tag, EmptyData), without
        # computing it.
        entries = sorted({entry for entry, _ in before[function]})
        cached = second.nodes[label].get_cached_results(entries)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
        pairs = cast(dict[bytes, tuple[TagProtocol, Any]], cached)
        assert len(pairs) == 2
        misses = {
            _record_id(tag)
            for tag, data in pairs.values()
            if isinstance(data, EmptyData)
        }
        assert misses == (
            {"row_0", "row_1"} if scenario == ALL_MISSING else {"row_1"}
        ), label
        for tag, _ in pairs.values():
            # Hit or miss, the tag is session 1's: types, nullability, metadata (no
            # storage-only data-tag mark), system tags, hash and values.
            record_id, types, tag_hash, values = _tag_facts(tag)
            want_types, want_hash, want_values, _ = presented[label][record_id]
            assert types == want_types, (label, record_id)
            assert tag_hash == want_hash, (label, record_id)
            assert values == want_values, (label, record_id)
            assert _fields(tag) == first_fields[label][record_id], (label, record_id)

    summary = second.run(orchestrator=run())
    recomputed = 2 if scenario == ALL_MISSING else 1
    assert {
        label: (summary.nodes[label].computed, summary.nodes[label].reused)
        for label in ("one", "two")
    } == {
        "one": (recomputed, 2 - recomputed),
        "two": (recomputed, 2 - recomputed) if config == BOTH_EPHEMERAL else (0, 2),
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
