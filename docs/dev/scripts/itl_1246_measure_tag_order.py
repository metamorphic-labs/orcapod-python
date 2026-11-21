"""Measure which data-tag identities depend on the order of a stream's tag columns.

ITL-1246 / ITL-1227. Builds one table under every order of its tag columns (three
data-tags ``a``, ``b``, ``c`` and one plain tag ``p``) and prints, for each element, the
number of distinct values across the orders (1 means order-free). Run with
``uv run python docs/dev/scripts/itl_1246_measure_tag_order.py``.
"""

from __future__ import annotations

import asyncio
import itertools
import warnings
from collections import defaultdict
from typing import Any

import pyarrow as pa

warnings.simplefilter("ignore")

import orcapod as op  # noqa: E402
from orcapod.core.streams import ArrowTableStream  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from orcapod.sources import ArrowTableSource  # noqa: E402

TAGS = ("a", "b", "c")
ROWS = {
    "a": [1, 1, 2, 2],
    "b": ["x", "y", "x", "y"],
    "c": [True, False, False, True],
    "p": ["p1", "p2", "p3", "p4"],
    "v": [10, 20, 30, 40],
    "w": [1.5, 2.5, 3.5, 4.5],
}


@op.function_pod(output_keys="doubled")
def doubled(v: int) -> int:
    return 2 * v


TYPES = {
    "a": pa.int64(),
    "b": pa.large_string(),
    "c": pa.bool_(),
    "p": pa.large_string(),
    "v": pa.int64(),
    "w": pa.float64(),
}


LAYOUT: list[tuple[str, ...]] = [("a", "b", "c")]


def table() -> pa.Table:
    """The table, its data-tag columns physically laid out in ``LAYOUT[0]`` order."""
    names = [*LAYOUT[0], "p", "v", "w"]
    schema = pa.schema([pa.field(k, TYPES[k], nullable=False) for k in names])
    return pa.table({k: pa.array(ROWS[k], TYPES[k]) for k in names}, schema=schema)


def small(columns: dict[str, list[Any]]) -> pa.Table:
    schema = pa.schema(
        [pa.field(k, TYPES.get(k, pa.int64()), nullable=False) for k in columns]
    )
    return pa.table(
        {k: pa.array(v, schema.field(k).type) for k, v in columns.items()},
        schema=schema,
    )


def source(order: tuple[str, ...], source_id: str = "src") -> Any:
    return ArrowTableSource(
        table(),
        tag_columns=[*order, "p"],
        tag_only_columns=["p"],
        source_id=source_id,
    )


def summary(stream: Any) -> dict[str, Any]:
    tag_keys, _ = stream.keys()
    all_tags, _ = stream.keys(columns={"system_tags": True})
    sys_cols = tuple(k for k in all_tags if k not in tag_keys)
    rows = {}
    for tag, data in stream.iter_data():
        key = repr(sorted(tag.as_dict().items()))
        rows[key] = (
            tag.content_hash().to_string(),
            data.content_hash().to_string(),
            tuple(sorted((k, str(v)) for k, v in tag.as_dict(all_info=True).items())),
        )
    return {
        "content_hash": stream.content_hash().to_string(),
        "pipeline_hash": stream.pipeline_hash().to_string(),
        "system_tag_columns": sys_cols,
        "rows": tuple(sorted(rows.items())),
    }


def saved_fall_through(job: Any) -> tuple[Any, ...]:
    import json
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "job.json"
        job.save(path)
        text = path.read_text()
    found: list[Any] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if "fall_through_tags" in node:
                found.append(tuple(node["fall_through_tags"]))
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(json.loads(text))
    return tuple(found)


def job_summary(order: tuple[str, ...]) -> dict[str, Any]:
    db = InMemoryArrowDatabase()
    job = op.PipelineJob(name="m", database=db)
    with job:
        doubled.pod(source(order), label="f")
    job.run()
    async_db = InMemoryArrowDatabase()
    async_job = op.PipelineJob(name="m", database=async_db)
    with async_job:
        doubled.pod(source(order), label="f")
    asyncio.run(async_job.run_async())
    paths = tuple(sorted("/".join(p) for p in db.list_sources()))
    out: dict[str, Any] = {
        "table_paths": paths,
        "async table_paths": tuple(
            sorted("/".join(p) for p in async_db.list_sources())
        ),
        "saved fall_through_tags": saved_fall_through(job),
    }
    for label in sorted(job.nodes):
        s = summary(job.nodes[label])
        out.update({f"node[{label}].{k}": v for k, v in s.items()})
    for p in db.list_sources():
        recs = db.get_all_records(p, record_id_column="__rid")
        assert recs is not None
        name = "/".join(p)
        if "_result" in name:
            out[f"input_data_hashes[{name.split('/')[0]}..result]"] = tuple(
                sorted(str(v) for v in recs.column("__input_data_hash").to_pylist())
            )
        else:
            out["record_ids[pipeline]"] = tuple(
                sorted(str(v) for v in recs.column("__rid").to_pylist())
            )
    return out


def async_rows(stream: Any) -> tuple[Any, ...]:
    async def collect() -> list[Any]:
        return (
            [
                (tag.content_hash().to_string(), data.content_hash().to_string())
                async for tag, data in stream.async_iter_data()
            ]
            if hasattr(stream, "async_iter_data")
            else []
        )

    return tuple(sorted(asyncio.run(collect())))


def elements(order: tuple[str, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {}

    def put(prefix: str, s: dict[str, Any]) -> None:
        out.update({f"{prefix}.{k}": v for k, v in s.items()})

    src = source(order)
    put("source", summary(src))
    # Function pod over data-tags (fall-through), with and without an automatic select.
    put(
        "fall_through(v only)",
        summary(doubled.pod(src.select_data_columns([*order, "v"]))),
    )
    put("fall_through(auto select drops w)", summary(doubled.pod(src)))
    # Operators that carry data-tags.
    other = ArrowTableSource(
        small({"a": [1, 2], "b": ["x", "y"], "z": [7, 8]}),
        tag_columns=["a", "b"],
        source_id="other",
    )
    put("join", summary(src.join(other)))
    put("semi_join", summary(src.semi_join(other)))
    from orcapod.operators import MergeJoin

    other_w = ArrowTableSource(
        small({"a": [1, 2], "b": ["x", "y"], "c": [True, False], "w": [9.5, 8.5]}),
        tag_columns=["a", "b", "c"],
        source_id="other_w",
    )
    put("merge_join", summary(MergeJoin()(src, other_w)))
    put("group_by(a)", summary(src.group_by("a")))
    put("batch(2)", summary(src.batch(2)))
    put("tag_by(v,w)", summary(src.tag_by(["v", "w"])))
    put("map_tags(a->aa)", summary(src.map_tags({"a": "aa"})))
    put("drop_nulls()", summary(src.drop_nulls()))
    # A raw stream: no producer.
    raw = ArrowTableStream(table(), tag_columns=[*order, "p"], data_tags=order)
    put("raw(data_tags)", summary(raw))
    raw_plain = ArrowTableStream(table(), tag_columns=[*order, "p"])
    put("raw(plain tags)", summary(raw_plain))
    put("job", job_summary(order))
    return out


def main() -> None:
    # by_tag_order[key]: the most distinct values over the tag orders, any one layout.
    # by_layout[key]: the most distinct values over the layouts, any one tag order.
    seen: dict[str, dict[tuple[Any, Any], str]] = defaultdict(dict)
    orders = list(itertools.permutations(TAGS))
    for layout in orders:
        LAYOUT[0] = layout
        for order in orders:
            for key, value in elements(order).items():
                seen[key][(layout, order)] = repr(value)
    width = max(len(k) for k in seen)
    print(f"{'element':<{width}}  tag-order  layout  all (of 36)")
    for key, cells in seen.items():
        by_order = max(len({cells[(lay, o)] for o in orders}) for lay in orders)
        by_layout = max(len({cells[(lay, o)] for lay in orders}) for o in orders)
        total = len(set(cells.values()))
        flag = "" if total == 1 else "   <--"
        print(f"{key:<{width}}  {by_order:>9}  {by_layout:>6}  {total:>3}{flag}")


if __name__ == "__main__":
    main()
