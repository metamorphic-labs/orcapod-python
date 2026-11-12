"""A v0.2 store under v0.3: refused explicitly, never damaged (ITL-888, ITL-910).

The array format changed in v0.3 without a migration (ITL-888), and so did every Arrow-derived
hash (starfix hash format 2, label ``arrow_v0.2``, ITL-910). Rows a v0.2 store holds are keyed
under ``arrow_v0.1`` and can never match v0.3 inputs, so a v0.3 job refuses the store with
``StoreHashLabelError`` before computing or writing anything, instead of computing next to the
old rows. Detection of old stores beyond that and their migration are deferred (ITL-889).
"""

from __future__ import annotations

import sys
import tarfile
from pathlib import Path

from typing import Any

import deltalake
import pytest

from orcapod.errors import StoreHashLabelError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from tests.test_arrays.v02_store_pods import CALLS, build_job

FIXTURE = Path(__file__).parent / "data" / "v02_array_store.tar.gz"


def _files(root: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()
    }


def _tables(root: Path) -> set[str]:
    return {str(p.parent.relative_to(root)) for p in root.rglob("_delta_log")}


def _local_name(name: str) -> str:
    # Windows forbids ':' in file names; DeltaTableDatabase stores it as '!' there.
    # Unverified on Windows: CI runs on Linux only.
    return name.replace(":", "!") if sys.platform == "win32" else name


def _local_data_filter(member: tarfile.TarInfo, dest: str) -> tarfile.TarInfo | None:
    return tarfile.data_filter(member.replace(name=_local_name(member.name)), dest)


@pytest.fixture
def store(tmp_path: Path) -> Path:
    target = tmp_path / "store"
    with tarfile.open(FIXTURE) as tar:
        if hasattr(tarfile, "data_filter"):
            tar.extractall(target, filter=_local_data_filter)
        else:  # Python 3.11.0-3.11.3 have no extraction filters
            for member in tar.getmembers():
                member.name = _local_name(member.name)
                path = Path(member.name)
                if (
                    path.is_absolute()
                    or ".." in path.parts
                    or not (member.isfile() or member.isdir())
                ):
                    raise ValueError(f"unsafe member in {FIXTURE}: {member.name!r}")
            tar.extractall(target)  # every member was checked above
    return target


@pytest.fixture(params=["sync", "async"])
def orchestrator(request: pytest.FixtureRequest) -> Any:
    return {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}[
        request.param
    ]()


def test_v02_store_is_refused_before_anything_runs(
    store: Path, orchestrator: Any
) -> None:
    """A v0.3 run on a v0.2 store raises before computing or writing anything.

    The array nodes would write new tables (their pipeline hashes moved, ITL-888), but the
    array-free ``plus_one`` and ``after_drop`` nodes and ``total``'s result table hold v0.2
    rows, so the whole job is refused before its first node runs.
    """
    files_before = _files(store)
    CALLS.clear()

    job = build_job(store)
    with pytest.raises(StoreHashLabelError) as info:
        job.run(orchestrator=orchestrator)

    assert info.value.found_label == "arrow_v0.1"
    assert info.value.current_label == "arrow_v0.2"
    assert CALLS == []
    assert _files(store) == files_before

    # Every table is still readable, read the way Orcapod's Delta database reads it.
    for table in _tables(store):
        dt = deltalake.DeltaTable(str(store / table))
        assert dt.to_pyarrow_dataset(as_large_types=True).to_table().num_rows > 0


@pytest.mark.parametrize("label", ["plus_one", "after_drop", "total"])
def test_reading_a_node_with_v02_rows_raises(store: Path, label: str) -> None:
    """Reading a node whose pipeline or result table holds v0.2 rows raises.

    ``total``'s pipeline table is new under v0.3, but its result table is the v0.2 one.
    """
    files_before = _files(store)
    node = build_job(store).nodes[label]
    with pytest.raises(StoreHashLabelError):
        node.as_table()
    assert _files(store) == files_before
