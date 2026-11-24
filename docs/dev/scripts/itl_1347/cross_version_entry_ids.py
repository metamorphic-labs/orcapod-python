"""Cross-version check of a function node's entry ids (ITL-1361).

Run once per Orcapod version, each with its own environment, over the same inputs:

    PYTHONPATH=<repository root> <env>/bin/python cross_version_entry_ids.py <store dir> write <out.json>
    PYTHONPATH=<repository root> <other env>/bin/python cross_version_entry_ids.py <store dir> load <out.json>

(not ``-I``: it ignores ``PYTHONPATH``, which the fixture import needs).

``write`` runs a function job node over each input and stores its results in a Delta
store; ``load`` runs the same node load-only over the same store. A store written by one
version must be hit, row for row, by the other: same stored base entry ids, every row
from the store, no call of the function. The inputs cover the several-items family
(tag order, layout, tag roles), list-valued columns after ``Batch`` and ``GroupBy``,
nested source info, nullable and logical values, and raw streams whose rows type their
system tags or columns otherwise (an ``int32`` system tag, a nullable one, an untyped
``null`` data column), each as a job hands it on and as a raw ``ArrowTableStream``.
Needs the repository root on ``PYTHONPATH`` (for ``tests.test_core.operators.several_items``).
"""

from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path
from typing import Any

import pyarrow as pa

from orcapod import function_pod
from orcapod.core.data_tags import stream_data_tags
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase
from orcapod.operators import Batch, GroupBy
from orcapod.pipeline.observer import NoOpObserver
from orcapod.sources import DictSource
from tests.test_core.operators.several_items import MAIN, arrangements, make_source

warnings.simplefilter("ignore")
CALLS: list[Any] = []
DECLARED: dict[str, Any] = {}


@function_pod(output_keys="y")
def flat(d1: int | None) -> int:
    CALLS.append(d1)
    return -1 if d1 is None else d1 + 1


@function_pod(output_keys="y")
def listed(d1: list[int | None] | None) -> int:
    CALLS.append(d1)
    return -1 if d1 is None else len(d1)


@function_pod(output_keys="y")
def nullable(i: int | None) -> int:
    CALLS.append(i)
    return -1 if i is None else i


@function_pod(output_keys="y")
def untyped(x: Any) -> int:
    CALLS.append(x)
    return -1 if x is None else x


class Cached(NoOpObserver):
    def __init__(self) -> None:
        self.cached: list[bool] = []

    def contextualize(self, *identity_path: str) -> Cached:
        return self

    def on_data_end(self, *args: Any, **kwargs: Any) -> None:
        self.cached.append(bool(kwargs["cached"] if "cached" in kwargs else args[-1]))


def forms(stream: Any) -> dict[str, Any]:
    rows = list(stream.iter_data())
    handed_on = StaticOutputOperatorPod._materialize_upstream(rows, stream)
    raw = ArrowTableStream(
        stream.as_table(columns={"system_tags": True, "source": True}),
        tag_columns=stream.keys()[0],
        data_tags=stream_data_tags(stream),
    )
    return {"handed_on": handed_on, "raw": raw}


def cases() -> dict[str, tuple[Any, Any]]:
    out: dict[str, tuple[Any, Any]] = {}
    for arrangement in arrangements(MAIN):
        stream = make_source(MAIN, arrangement).select_data_columns(["d1"])
        for name, form in forms(stream).items():
            out[f"family/{arrangement.id}/{name}"] = (flat, form)
    for arrangement in arrangements(MAIN)[::4]:
        source = make_source(MAIN, arrangement)
        for kind, stream in (
            ("batch", Batch(batch_size=2)(source)),
            ("group_by", GroupBy(by=[arrangement.tag_order[0]])(source)),
        ):
            selected = stream.select_data_columns(["d1"])
            for name, form in forms(selected).items():
                out[f"{kind}/{arrangement.id}/{name}"] = (listed, form)
    values = DictSource(
        [{"a": 1, "b": "x", "i": 1}, {"a": 2, "b": "y", "i": None}],
        tag_columns=["a", "b"],
        tag_only_columns=True,
        source_id="values",
    )
    for name, form in forms(values).items():
        out[f"nullable/{name}"] = (nullable, form)
    raw_tables = {
        "int32_system_tag": pa.table(
            {"k": ["a", "b"], "d1": [1, 2], "e": pa.array([10, 11], pa.int32())}
        ),
        "nullable_system_tag": pa.table(
            {
                "k": ["a", "b"],
                "d1": [1, 2],
                "e": pa.array(["s", None], pa.large_string()),
            }
        ),
    }
    for name, table in raw_tables.items():
        out[f"raw/{name}"] = (
            flat,
            ArrowTableStream(table, tag_columns=["k"], system_tag_columns=["e"]),
        )
    out["raw/meta_column"] = (
        flat,
        ArrowTableStream(
            pa.table({"k": ["a", "b"], "d1": [1, 2], "__review": [10, 11]}),
            tag_columns=["k"],
        ),
    )
    # The node is declared against the clean input and executed on the raw one.
    DECLARED["raw/meta_column"] = ArrowTableStream(
        pa.table({"k": ["a", "b"], "d1": [1, 2]}), tag_columns=["k"]
    )
    extension = {b"ARROW:extension:name": b"file", b"ARROW:extension:metadata": b""}
    out["raw/extension_metadata_system_tag"] = (
        flat,
        ArrowTableStream(
            pa.Table.from_arrays(
                [
                    pa.array(["a", "b"]),
                    pa.array([1, 2]),
                    pa.array(["s", "t"], pa.large_string()),
                ],
                schema=pa.schema(
                    [
                        pa.field("k", pa.large_string()),
                        pa.field("d1", pa.int64()),
                        pa.field(
                            "_tag_s",
                            pa.large_string(),
                            nullable=False,
                            metadata=extension,
                        ),
                    ]
                ),
            ),
            tag_columns=["k"],
        ),
    )
    out["raw/null_in_non_null_system_tag"] = (
        flat,
        ArrowTableStream(
            pa.Table.from_arrays(
                [
                    pa.array(["a", "b"]),
                    pa.array([1, 2]),
                    pa.array(["s", None], pa.large_string()),
                ],
                schema=pa.schema(
                    [
                        pa.field("k", pa.large_string()),
                        pa.field("d1", pa.int64()),
                        pa.field("_tag_extra", pa.large_string(), nullable=False),
                    ]
                ),
            ),
            tag_columns=["k"],
        ),
    )
    out["raw/null_data_column"] = (
        untyped,
        ArrowTableStream(
            pa.table({"k": ["a", "b"], "x": pa.nulls(2)}), tag_columns=["k"]
        ),
    )
    return out


def main() -> None:
    store, mode, report = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
    if mode not in ("write", "load"):
        # Checked before any store is opened: anything else would compute and write.
        raise SystemExit(f"mode must be 'write' or 'load', not {mode!r}")
    results: dict[str, Any] = {}
    for index, (label, (pod, stream)) in enumerate(sorted(cases().items())):
        database = DeltaTableDatabase(store / f"case_{index:03d}")
        observer = Cached()
        CALLS.clear()
        try:
            node = FunctionJobNode(
                pod.pod, DECLARED.get(label, stream), pipeline_database=database
            )
            node.execute(stream, observer=observer, load_only=(mode == "load"))
            database.flush()
            ids: list[str] = []
            for path in database.list_table_paths():
                records = database.get_all_records(path)
                if (
                    records is None
                    or "__pipeline_base_entry_id" not in records.column_names
                ):
                    continue
                ids += [
                    value.hex()
                    for value in records["__pipeline_base_entry_id"].to_pylist()
                    if isinstance(value, bytes)
                ]
            ids.sort()
            results[label] = {
                "ids": ids,
                "cached": observer.cached,
                "calls": len(CALLS),
                # Which result each input received, in order: the write and load
                # reports of one case must hold the same.
                "received": [
                    [repr(tag.as_dict()), repr(data.as_dict())]
                    for tag, data in node.iter_data()
                ],
            }
        except Exception as error:  # noqa: BLE001  # the outcome is compared across versions
            results[label] = {"error": f"{type(error).__name__}: {error}"}
    report.write_text(json.dumps(results, indent=1, sort_keys=True))
    print(len(results), "cases")


if __name__ == "__main__":
    main()
