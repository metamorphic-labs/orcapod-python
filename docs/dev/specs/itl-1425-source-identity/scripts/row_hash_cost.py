"""Cost of hashing every row of a source (ITL-1425, requirement R6).

Hashes each row the way ``CachedSource`` does today: ``arrow_hasher.hash_table`` of a
one-row slice (``arrow_v1``; starfix has no per-row call, so one digest per row). Compares
it with the time to read the source and to hash the whole table once.

Two inputs: the tutorial dataset (``docs/tutorial/workshop_data.py``, its largest table:
every spikes file of every session stacked) and a synthetic table of N rows (default
1,000,000; one string key, one float, one integer).

Run from ``docs/dev/specs/itl-1425-source-identity``:
``uv run python -m scripts.row_hash_cost [N]``. One process; note the machine load.
"""

from __future__ import annotations

import importlib
import os
import sys
import tempfile
import time
import warnings
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv

from orcapod.sources import ArrowTableSource, CSVSource

warnings.simplefilter("ignore")

HERE = Path(__file__).resolve()
TUTORIAL = HERE.parents[5] / "docs" / "tutorial"


def timed(fn):
    start = time.perf_counter()
    out = fn()
    return out, time.perf_counter() - start


def per_row_hashes(table: pa.Table, hasher) -> list[bytes]:
    out = []
    for batch in table.to_batches():
        for i in range(batch.num_rows):
            out.append(hasher.hash_table(batch.slice(i, 1)).to_prefixed_digest())
    return out


def measure(name: str, build) -> None:
    """``build`` constructs the source (which reads the data) and returns it."""
    source, t_build = timed(build)
    table, t_as_table = timed(
        lambda: source.as_table(columns={"system_tags": True, "source": True})
    )
    t_read = t_build + t_as_table
    hasher = source.data_context.arrow_hasher
    _, t_whole = timed(lambda: hasher.hash_table(table))
    hashes, t_rows = timed(lambda: per_row_hashes(table, hasher))
    n = table.num_rows
    print(f"-- {name}: {n:,} rows, {table.num_columns} columns")
    print(
        f"   build the source and read it (system tags and tokens):     {t_read:8.3f} s"
    )
    print(
        f"   hash the whole table once (hash_table):                     {t_whole:8.3f} s"
    )
    print(
        f"   hash every row (one hash_table per one-row slice):          {t_rows:8.3f} s"
        f"  ({t_rows / n * 1e6:.1f} us/row, {t_rows / max(t_read, 1e-9):.1f}x the read)"
    )
    print(f"   distinct row hashes: {len(set(hashes)):,}")


def tutorial(folder: Path) -> None:
    sys.path.insert(0, str(TUTORIAL))
    make_dataset = importlib.import_module("workshop_data").make_dataset
    root = make_dataset(folder / "workshop_data")
    parts = []
    for f in sorted((root / "sessions").glob("*/spikes_*.csv")):
        t = pacsv.read_csv(f)
        session = f.parent.name
        parts.append(
            t.append_column("session", pa.array([session] * t.num_rows)).append_column(
                "probe", pa.array([f.stem] * t.num_rows)
            )
        )
    spikes = pa.concat_tables(parts)
    stacked = folder / "spikes_all.csv"
    pacsv.write_csv(spikes, stacked)
    measure(
        "tutorial spikes (CSVSource, every spikes file stacked)",
        lambda: CSVSource(
            str(stacked),
            source_id="spikes",
            tag_columns=["session", "probe"],
            tag_only_columns=True,
        ),
    )
    for name in ("subjects.csv", "sessions.csv", "units.csv"):
        measure(
            f"tutorial {name}",
            lambda name=name: CSVSource(str(root / name), source_id=name),
        )


def synthetic(n: int) -> None:
    table = pa.table(
        {
            "key": pa.array([f"k{i:07d}" for i in range(n)], pa.large_string()),
            "x": pa.array([i * 0.5 for i in range(n)], pa.float64()),
            "y": pa.array(range(n), pa.int64()),
        }
    )
    measure(
        "synthetic ArrowTableSource keyed by 'key'",
        lambda: ArrowTableSource(
            table,
            tag_columns=["key"],
            record_id_columns=["key"],
            tag_only_columns=True,
            source_id="synthetic",
        ),
    )


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    print("machine: cpus", os.cpu_count(), "| load averages", os.getloadavg())
    with tempfile.TemporaryDirectory() as d:
        tutorial(Path(d))
    synthetic(n)
    print("machine load after:", os.getloadavg())


if __name__ == "__main__":
    main()
