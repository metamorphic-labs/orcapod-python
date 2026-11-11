"""A v0.2 store holding ``.npy`` arrays under v0.3: recomputed into new tables, never damaged (ITL-888).

The array format changed in v0.3 without a migration. Because the logical type
name moved to ``orcapod.ndarray``, every node with an array column gets a new
pipeline hash, so v0.3 writes new tables for those nodes and leaves the v0.2
tables as they were. Detection of old stores and their migration are deferred.
"""

from __future__ import annotations

import tarfile
from pathlib import Path

import deltalake
import numpy as np
import pytest

from tests.test_arrays.v02_store_pods import ARRAYS, build_job

FIXTURE = Path(__file__).parent / "data" / "v02_array_store.tar.gz"


def _files(root: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()
    }


def _tables(root: Path) -> set[str]:
    return {str(p.parent.relative_to(root)) for p in root.rglob("_delta_log")}


def _node_of(table: str) -> str:
    parts = Path(table).parts
    return parts[2] if parts[1] == "_result" else parts[1]


@pytest.fixture
def store(tmp_path: Path) -> Path:
    target = tmp_path / "store"
    with tarfile.open(FIXTURE) as tar:
        tar.extractall(target, filter="data")
    return target


def test_v02_store_is_left_intact_and_array_nodes_get_new_tables(store: Path) -> None:
    files_before = _files(store)
    tables_before = _tables(store)

    job = build_job(store)
    job.run()

    # Results are correct and complete under v0.3.
    doubled = {
        t.as_dict()["k"]: d.as_dict()["y"] for t, d in job.nodes["double"].iter_data()
    }
    assert sorted(doubled) == sorted(ARRAYS)
    for k, x in ARRAYS.items():
        assert doubled[k].dtype == x.dtype and np.array_equal(doubled[k], x * 2)
    for label in ("total", "direct_sum", "plus_one"):
        assert sum(1 for _ in job.nodes[label].iter_data()) == len(ARRAYS)

    # Nothing written by v0.2 was changed or removed.
    files_after = _files(store)
    for path, content in files_before.items():
        assert files_after.get(path) == content, path

    # Array nodes moved to new pipeline tables; double's result table moved too
    # (its function identity includes the ndarray output). The array-free node did not.
    new_tables = _tables(store) - tables_before
    assert {_node_of(t) for t in new_tables if "/_result/" not in f"/{t}"} == {
        "double",
        "total",
        "direct_sum",
    }
    assert {_node_of(t) for t in new_tables if "/_result/" in f"/{t}"} == {"double"}
    plus_one_files = {p for p in files_after if _node_of(p) == "plus_one"}
    assert plus_one_files == {p for p in files_before if _node_of(p) == "plus_one"}

    # Every table, old and new, is still readable, read the way Orcapod's Delta
    # database reads it (large Arrow types).
    for table in _tables(store):
        dt = deltalake.DeltaTable(str(store / table))
        assert dt.to_pyarrow_dataset(as_large_types=True).to_table().num_rows > 0


def test_v02_array_column_is_not_mixed_with_the_new_format(store: Path) -> None:
    build_job(store).run()
    for table in _tables(store):
        schema = deltalake.DeltaTable(str(store / table)).schema().to_arrow()
        text = str(schema)
        assert not ("numpy.ndarray" in text and "orcapod.ndarray" in text), table
