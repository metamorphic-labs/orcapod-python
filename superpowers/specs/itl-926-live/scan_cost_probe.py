"""READ-ONLY: where the time goes when a small Spiral table is slow to scan.

Run after ``spiral login``, from the repository root::

    uv run python superpowers/specs/itl-926-live/scan_cost_probe.py edi-sample-v1-855632 \
        unit unit_location > scan_cost.txt

For each table it prints the number of column groups and fragments (from the manifests, no
data scan), then times three scans: key columns only, the full table, and the full table
again (to see whether a second read is cached). It reads nothing else and writes nothing.
"""

from __future__ import annotations

import signal
import sys
import time


def main() -> None:
    signal.alarm(900)
    import spiral

    project_id, tables = sys.argv[1], sys.argv[2:]
    sp = spiral.Spiral()
    project = sp.project(project_id)
    for name in tables:
        table = project.table(f"default.{name}")
        snapshot = table.core.get_snapshot(asof=None)
        stats = sp.core.manifest_stats(snapshot)
        groups = stats.column_group_stats()
        print(f"## {name}")
        print(
            f"  column groups: {len(groups)}; key-space fragments: "
            f"{stats.key_space_stats.total_fragments} "
            f"(L0 {stats.key_space_stats.l0_fragments})"
        )
        for group, group_stats in groups:
            print(
                f"    {group}: {group_stats.total_fragments} fragments "
                f"(L0 {group_stats.l0_fragments}), {group_stats.total_rows} rows"
            )
        keys = list(table.key_schema.names)
        for label, expr in (
            ("keys only", table.select(*keys)),
            ("full", table.select()),
            ("full again", table.select()),
        ):
            start = time.time()
            rows = sp.scan(expr).to_table().num_rows
            print(f"  scan {label:10}: {rows} rows in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
