"""The identity structure of every source kind that can be built here (ITL-1425).

Prints ``identity_structure()`` (what the content hash is built from) per kind, with
whether it holds the class name, the ``source_id``, the Arrow schema identity and the key
columns. Kinds that need a server (PostgreSQL, SpiralDB, a generic ``DBTableSource``) are
not built; the document cites their code instead. ``SourceProxy`` is shown through a
saved and loaded job.

Run from ``docs/dev/specs/itl-1425-source-identity``:
``uv run python -m scripts.identity_structures``.
"""

from __future__ import annotations

import tempfile
import warnings
from pathlib import Path

import pyarrow as pa

from orcapod import PipelineJob
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import CachedSource, ListSource, PollingSource
from orcapod.types import ContentHash, Schema

from ._fixtures import same_rows_sources, scratch
from .stand_in_job import ratio

warnings.simplefilter("ignore")


class ToyPolling:
    """A minimal ``DynamicSourceProtocol`` implementation; never polled here."""

    def identity(self):  # type: ignore[no-untyped-def]
        return ("ToyPolling", "endpoint-a")

    def to_config(self):  # type: ignore[no-untyped-def]
        return {}

    @classmethod
    def from_config(cls, config):  # type: ignore[no-untyped-def]
        return cls()

    def schema(self) -> Schema:
        return Schema({"subject": str, "weight": float})

    async def poll(self, cursor=None):  # type: ignore[no-untyped-def]
        return False

    async def fetch(self, cursor=None):  # type: ignore[no-untyped-def]
        raise NotImplementedError

    async def close(self) -> None:
        return None


def describe(kind: str, src) -> None:
    structure = src.identity_structure()
    flat = structure if isinstance(structure, tuple) else (structure,)
    has_class = type(src).__name__ in flat or any(
        isinstance(x, str) and x.endswith("Source") for x in flat
    )
    has_id = src.source_id in flat
    has_schema = any(
        isinstance(x, tuple)
        and len(x) == 2
        and all(isinstance(y, ContentHash) for y in x)
        for x in flat
    )

    def show(x):  # type: ignore[no-untyped-def]
        if isinstance(x, ContentHash):
            return f"<{x.method}:{x.to_hex()[:8]}>"
        if isinstance(x, tuple):
            return "(" + ", ".join(show(y) for y in x) + ")"
        return repr(x)

    print(f"-- {kind}  [{type(src).__name__}]")
    print(f"   structure: {show(structure)}")
    print(
        f"   class name: {has_class}; source_id ({src.source_id!r}): {has_id}; "
        f"Arrow schema identity: {has_schema}"
    )


def main() -> None:
    folder = scratch()
    sources = same_rows_sources(folder)
    for kind, src in sources.items():
        describe(kind, src)
    describe(
        "list",
        ListSource(
            "weight",
            [25.5, 22.0],
            source_id="subjects",
            tag_function=lambda v, i: {"subject": f"s{i}"},
            expected_tag_keys=["subject"],
        ),
    )
    describe(
        "cached (over arrow_table)",
        CachedSource(
            sources["arrow_table"], cache_database=DeltaTableDatabase(folder / "c")
        ),
    )

    db = DeltaTableDatabase(folder / "db")
    job = PipelineJob(name="ids", database=db)
    with job:
        ratio.pod(sources["arrow_table"], label="ratio")
    job.run()
    node = job.nodes["ratio"]
    describe("derived, plain (node.as_source())", node.as_source())
    describe(
        "derived, tags_as_data",
        node.as_source(tags_as_data={"subject": "subject_copy"}, source_id="ratios"),
    )

    describe(
        "polling (toy impl)",
        PollingSource(ToyPolling(), tag_columns="subject", record_id_columns="subject"),
    )
    described = PollingSource(
        ToyPolling(),
        tag_columns="subject",
        record_id_columns="subject",
        source_id="subjects",
    )
    describe("polling (toy impl, given source_id)", described)

    print("\n== Saved and loaded jobs: what the slot holds ==")
    for kind in ("csv", "arrow_table"):
        saved = PipelineJob(
            name="ids", database=DeltaTableDatabase(folder / f"db_{kind}")
        )
        with saved:
            ratio.pod(sources[kind], label="ratio")
        saved.run()
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "job.json"
            saved.save(path)
            loaded = PipelineJob.load(path)
        slot = loaded.nodes["subjects"]
        bound = slot.bound_source
        print(
            f"   {kind:12s} loaded slot bound to: {type(bound).__name__}; slot content "
            f"hash equal to the source's: {slot.content_hash() == sources[kind].content_hash()}"
        )
    _ = pa


if __name__ == "__main__":
    main()
