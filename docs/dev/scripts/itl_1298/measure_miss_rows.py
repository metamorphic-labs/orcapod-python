"""ITL-1298: a cross-session miss row's tag (ephemeral results gone in a later session).

usage: uv run python docs/dev/scripts/itl_1298/measure_miss_rows.py

Session 1: a job over an in-memory database with an ephemeral in-memory database, the
nodes' results ephemeral (``NodeConfig(is_result_ephemeral=True)``): source (tags
``i8: int8``, ``f32: float32``, ``s: large_string``, ``st: struct``, ``l: large_list``,
``p: Path``; data ``x``) -> ``inc`` (node ``one``) -> ``dbl`` (node ``two``). Session 2: a
new job over the same database with a NEW, empty ephemeral database: every ephemeral
result is a cross-session miss. For each session: the run's computed / reused counts;
per row of node ``one`` and ``two`` as presented: the tag's Arrow types, tag content
hash, record id and base entry id; the stored pipeline rows (count, base entry ids,
recomputation indices); and, in session 2 before the run, the rows ``one`` loads from
storage (``_load_cached_entries``: the miss rows' tags as rebuilt), then a
``run(load_only=True)`` in yet another job.

Measured on 2026-11-19 on ``main`` (the miss rows' tags were rebuilt from Python values:
``int8`` and ``float32`` came back as 64-bit types with another tag hash, and a struct tag
raised when read) and with a prototype of the fix; run here with the fix (ITL-1298), the
miss rows' tags have the stored types and session 1's tag hashes.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import SyncPipelineOrchestrator
from orcapod.system_constants import constants
from orcapod.types import NodeConfig

CTX = get_default_context()


@function_pod(output_keys="y")
def inc(x: int) -> int:
    return x + 1


@function_pod(output_keys="z")
def dbl(y: int) -> int:
    return 2 * y


def source(with_struct: bool = True) -> ArrowTableSource:
    path_type = cast(
        pa.BaseExtensionType, CTX.type_converter.python_type_to_arrow_type(Path)
    )
    arrays = {
        "i8": pa.array([1, 2], pa.int8()),
        "f32": pa.array([0.5, 1.5], pa.float32()),
        "s": pa.array(["é", "b"], pa.large_string()),
        "st": pa.array(
            [{"a": 1, "b": "u"}, {"a": 2, "b": "v"}],
            pa.struct([("a", pa.int64()), ("b", pa.large_string())]),
        ),
        "l": pa.array([[1, 2], [3]], pa.large_list(pa.int64())),
        "p": pa.ExtensionArray.from_storage(
            path_type, pa.array(["/a", "/b"], pa.large_string())
        ),
        "x": pa.array([10, 20], pa.int64()),
    }
    if not with_struct:
        del arrays["st"]
    schema = pa.schema(pa.field(n, a.type, nullable=False) for n, a in arrays.items())
    return ArrowTableSource(
        pa.Table.from_arrays(list(arrays.values()), schema=schema),
        tag_columns=[c for c in ("i8", "f32", "s", "st", "l", "p") if c in arrays],
        tag_only_columns=True,
        source_id="s",
    )


def build(
    db: Any, ephemeral: Any, with_struct: bool, only_one: bool = False
) -> PipelineJob:
    """``only_one``: only node ``one``'s results are ephemeral; ``two``'s persist."""
    job = PipelineJob(name="itl1298", database=db)
    with job:
        dbl.pod(inc.pod(source(with_struct), label="one"), label="two")
    job.set_ephemeral_database(ephemeral)
    if only_one:
        job.nodes["one"].node_config = NodeConfig(is_result_ephemeral=True)  # pyright: ignore[reportAttributeAccessIssue]
    else:
        job.apply_node_config(NodeConfig(is_result_ephemeral=True))
    return job


def describe_tag(tag: Any) -> dict[str, Any]:
    try:
        schema = tag.arrow_schema()
        system = tag.system_tags()
        return {
            "types": {f.name: str(f.type) for f in schema},
            "hash": tag.content_hash().to_string()[:24],
            "record id": next(
                v for k, v in system.items() if k.startswith("_tag_record_id")
            ),
        }
    except Exception as error:  # noqa: BLE001
        return {"RAISES": f"{type(error).__name__}: {str(error)[:90]}"}


def presented(job: PipelineJob, label: str) -> list[dict[str, Any]]:
    out = []
    for tag, data in job.nodes[label].iter_data():
        info = describe_tag(tag)
        info["data"] = sorted(data.as_dict().items())
        out.append(info)
    return out


def stored(db: Any, function: str) -> list[tuple[Any, ...]]:
    db.flush()
    [path] = [
        p for p in db.list_table_paths() if p[1] == function and p[-1].startswith("pdb")
    ]
    table = db.get_all_records(path)
    return sorted(
        zip(
            [str(table.column("i8").type)] * table.num_rows,
            table.column("i8").to_pylist(),
            [
                b.hex()[-12:]
                for b in table.column("__pipeline_base_entry_id").to_pylist()
            ],
            table.column("__pipeline_recomputation_index").to_pylist(),
            table.column(constants.IS_EPHEMERAL_COL).to_pylist(),
        )
    )


def main() -> None:
    for with_struct, only_one in (
        (True, False),
        (False, False),
        (False, True),
        (True, True),
    ):
        print(
            f"######## tags {'with' if with_struct else 'without'} the struct tag; "
            + (
                "only 'one' ephemeral, 'two' persistent"
                if only_one
                else "both ephemeral"
            )
        )
        run_sessions(with_struct, only_one)


def run_sessions(with_struct: bool, only_one: bool) -> None:
    db = InMemoryArrowDatabase()
    sessions = [
        ("session 1", InMemoryArrowDatabase()),
        ("session 2 (new ephemeral)", InMemoryArrowDatabase()),
    ]
    for name, ephemeral in sessions:
        job = build(db, ephemeral, with_struct, only_one)
        if name.startswith("session 2"):
            loaded = job.nodes["one"]._load_cached_entries()  # noqa: SLF001
            print(f"== {name}: rows 'one' loads from storage before the run")
            for base, (tag, data) in sorted(loaded.items()):
                print(
                    f"   {base.hex()[-12:]} {type(data).__name__:10} {describe_tag(tag)}"
                )
            other = build(db, InMemoryArrowDatabase(), with_struct, only_one)
            try:
                other.run(orchestrator=SyncPipelineOrchestrator(), load_only=True)
                print(f"   load_only run: one presents {presented(other, 'one')}")
                print(f"   load_only run: two presents {presented(other, 'two')}")
            except Exception as error:  # noqa: BLE001
                print(
                    f"   load_only run RAISES {type(error).__name__}: {str(error)[:150]}"
                )
        try:
            summary = job.run(orchestrator=SyncPipelineOrchestrator())
            counts = {
                lbl: (summary.nodes[lbl].computed, summary.nodes[lbl].reused)
                for lbl in ("one", "two")
            }
        except Exception as error:  # noqa: BLE001
            print(f"== {name}: run RAISES {type(error).__name__}: {str(error)[:200]}")
            continue
        print(f"== {name}: (computed, reused) {counts}")
        for label in ("one", "two"):
            for row in presented(job, label):
                print(f"   {label} presents {row}")
        for function in ("inc", "dbl"):
            print(
                f"   stored {function} (i8 type, i8, base entry, recomputation, ephemeral): {stored(db, function)}"
            )


if __name__ == "__main__":
    main()
