"""C3: what a change of the array hash definition does to an existing store (Option B).

Run: ``uv run python superpowers/specs/array-tensor-storage-experiments/c3_hash_change_on_existing_store.py``

Option B keeps ``.npy`` storage and the table paths, and changes only how an array is hashed.
This script simulates that. It runs ``source -> double -> total`` on a Delta store in a
fresh process with today's hash. Then it runs the same job on the same store in another fresh
process where ``NumpyArrayHandler.handle`` is replaced by a canonical digest with a new label
(``ndarray-v2:sha256``, the c1 prototype). The replacement is a stand-in for a real change;
it touches nothing under ``src/``.

Claims tested:

* Pipeline hashes and table paths stay the same (they use Python types only).
* Under the new definition, are the old rows still readable, are the functions re-run, and
  are new rows appended beside the old ones? Does a node then show duplicates?
* Can the new keys be derived from stored values alone? Each node's new input hashes are
  compared with hashes computed from the stored upstream arrays, without calling a function.
* Do system tag values change too (source record ids)?

Raw Delta row counts are read in a separate process. Reading the store with ``deltalake``
left the interpreter hanging at exit in this environment, with or without Orcapod imported
(not investigated), so that process leaves with ``os._exit``.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import numpy as np

os.environ.setdefault("RUST_LOG", "error")

X = {
    "a": np.arange(6, dtype=np.float32).reshape(2, 3),
    "b": np.asfortranarray(np.arange(6, dtype=np.float64).reshape(3, 2)),
    "c": np.array([1, -2, 3], dtype=np.int8),
}


def canonical_handle(self: Any, obj: Any, hasher: Any) -> Any:
    """Stand-in for a changed definition: c1's canonical digest under a new label."""
    from orcapod.types import ContentHash

    le = obj.astype(obj.dtype.newbyteorder("<"), copy=False)
    buf = np.ascontiguousarray(le)
    header = json.dumps(
        {"dtype": le.dtype.str.lstrip("<|="), "shape": list(obj.shape)},
        separators=(",", ":"),
    ).encode()
    h = hashlib.sha256(len(header).to_bytes(4, "little") + header)
    if buf.size:
        h.update(buf.tobytes())
    return ContentHash(method="ndarray-v2:sha256", digest=h.digest())


def child_run(store: Path, counter: Path, definition: str) -> None:
    from orcapod import PipelineJob, function_pod
    from orcapod.core.datagrams.datagram import Datagram
    from orcapod.databases import DeltaTableDatabase
    from orcapod.hashing.semantic_hashing.builtin_handlers import NumpyArrayHandler
    from orcapod.sources import DictSource

    if definition == "new":
        NumpyArrayHandler.handle = canonical_handle  # pyright: ignore[reportAttributeAccessIssue]  # simulate a new hash definition in this process only

    def bump(name: str) -> None:
        with counter.open("a") as f:
            f.write(name + "\n")

    @function_pod(output_keys="y")
    def double(x: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
        bump("double")
        return np.asarray(x * 2)

    @function_pod(output_keys="total")
    def total(y: np.ndarray) -> float:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
        bump("total")
        return float(y.sum())

    before = counter.read_text().splitlines() if counter.exists() else []
    src = DictSource(
        data=[{"k": k, "x": v} for k, v in X.items()], tag_columns=["k"], source_id="s"
    )
    job = PipelineJob(name="hash_change", store=DeltaTableDatabase(base_path=store))
    with job:
        d = double.pod(src, label="double")
        total.pod(d, label="total")
    job.run()
    after = counter.read_text().splitlines() if counter.exists() else []
    calls = after[len(before) :]

    out: dict[str, Any] = {
        "calls": {n: calls.count(n) for n in ("double", "total")},
        "pipeline_hash": {
            label: job.nodes[label].pipeline_hash().to_hex()[:16]
            for label in ("double", "total")
        },
        "source_system_tags": sorted(
            str(v)[:16]
            for t, _ in src.iter_data()
            for k, v in t.as_dict(columns={"system_tags": True}).items()
            if k.startswith("_tag_record_id")
        ),
    }
    for label in ("double", "total"):
        out[f"{label}_iter_rows"] = sum(1 for _ in job.nodes[label].iter_data())
    # Derive, without calling any function, what each node's input hashes should be under
    # this process's definition: from the source values for `double`, and from the arrays
    # that `double` holds in the store (read through the node) for `total`.
    out["derived_double_inputs"] = sorted(
        Datagram({"x": v}).content_hash().to_prefixed_digest().hex() for v in X.values()
    )
    out["derived_total_inputs_from_stored_y"] = sorted(
        {
            Datagram({"y": d.as_dict()["y"]}).content_hash().to_prefixed_digest().hex()
            for _, d in job.nodes["double"].iter_data()
        }
    )
    print("JSON:" + json.dumps(out, sort_keys=True))


def child_count(store: Path) -> None:
    from deltalake import DeltaTable

    out: dict[str, Any] = {}
    for log in sorted(store.rglob("_delta_log")):
        t = DeltaTable(str(log.parent)).to_pyarrow_table()
        parts = log.parent.relative_to(store).parts
        label = parts[2] if parts[1] == "_result" else parts[1]
        name = f"{label}/{parts[-1]}"  # node label / table kind (pdb_v1 or rdb_v1)
        names = t.schema.names
        ids = t.column("__record_id").to_pylist() if "__record_id" in names else []
        hashes = (
            sorted(
                {
                    h.hex()
                    for h in t.column("__input_data_hash").to_pylist()
                    if h is not None
                }
            )
            if "__input_data_hash" in names
            else []
        )
        out[name] = {"rows": t.num_rows, "distinct_record_ids": len(set(ids))}
        if hashes:
            out[name]["input_hashes"] = hashes
    print("JSON:" + json.dumps(out, sort_keys=True), flush=True)
    # deltalake left this interpreter hanging at exit in this environment; leave hard.
    os._exit(0)


def call(*args: str) -> dict[str, Any]:
    proc = subprocess.run(
        [sys.executable, __file__, *args], capture_output=True, text=True, timeout=600
    )
    for line in proc.stdout.splitlines():
        if line.startswith("JSON:"):
            return json.loads(line[5:])
    raise RuntimeError(f"child {args} failed: {proc.stderr.strip()[-600:]}")


def report(status: str, name: str, observed: object) -> None:
    print(f"[{status}] {name}: {observed}")


def parent() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store, counter = Path(tmp) / "store", Path(tmp) / "calls.txt"
        old = call("--run", str(store), str(counter), "old")
        c1 = call("--count", str(store))
        new = call("--run", str(store), str(counter), "new")
        c2 = call("--count", str(store))
        again = call("--run", str(store), str(counter), "new")
        c3 = call("--count", str(store))

    report("INFO", "run 1, today's hash: function calls", old["calls"])
    report("INFO", "run 2, new definition, same store: function calls", new["calls"])
    report("INFO", "run 3, new definition again: function calls", again["calls"])
    report(
        "OK" if old["pipeline_hash"] == new["pipeline_hash"] else "INFO",
        "pipeline hashes (and so table paths) unchanged by the hash definition",
        old["pipeline_hash"] == new["pipeline_hash"],
    )
    report(
        "INFO",
        "source system tag (_tag_record_id) values unchanged by the hash definition",
        old["source_system_tags"] == new["source_system_tags"],
    )
    for label in ("double", "total"):
        report(
            "INFO",
            f"{label}: rows seen through iter_data, runs 1 / 2 / 3",
            f"{old[f'{label}_iter_rows']} / {new[f'{label}_iter_rows']} / "
            f"{again[f'{label}_iter_rows']}",
        )
    report(
        "INFO", "raw Delta tables after run 1 (rows, distinct record ids)", _brief(c1)
    )
    report("INFO", "raw Delta tables after run 2", _brief(c2))
    report("INFO", "raw Delta tables after run 3", _brief(c3))
    for label in ("double", "total"):
        h1 = set(c1.get(f"{label}/rdb_v1", {}).get("input_hashes", []))
        h2 = set(c2.get(f"{label}/rdb_v1", {}).get("input_hashes", []))
        added = h2 - h1
        derived = set(
            new[
                f"derived_{label}_inputs"
                if label == "double"
                else "derived_total_inputs_from_stored_y"
            ]
        )
        report(
            "INFO",
            f"{label}: run 2 added {len(added)} input hashes; equal to those derived "
            "from stored values without calling a function",
            added == derived,
        )


def _brief(counts: dict[str, Any]) -> dict[str, str]:
    return {
        k: f"{v['rows']} rows, {v['distinct_record_ids']} ids"
        for k, v in counts.items()
    }


if __name__ == "__main__":
    if sys.argv[1:2] == ["--run"]:
        child_run(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4])
    elif sys.argv[1:2] == ["--count"]:
        child_count(Path(sys.argv[2]))
    else:
        parent()
