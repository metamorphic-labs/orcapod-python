"""Compare a reader's record with what the live jobs showed (ITL-1388).

Usage: ``python analyze.py <workdir> <reader out.json> [...]``

For each node and each read, prints the rows shown against the rows the live job showed
after its last run (generation 3), as multisets: ``=live`` when equal; otherwise the
counts, and whether the extra rows are rows of earlier generations of the same job
(``+earlier runs``) or of the other job sharing the tables (``+OTHER JOB``).
"""

from __future__ import annotations

from typing import Any

import json
import sys
from collections import Counter
from pathlib import Path


def key(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True)


def compare(shown, live: list[Any], earlier: list[Any], other: list[Any]) -> str:
    if isinstance(shown, str):
        return shown.split("\n")[0][:140]
    s, lv = Counter(map(key, shown)), Counter(map(key, live))
    if s == lv:
        return f"=live ({len(shown)})"
    extra = s - lv
    missing = lv - s
    parts = [f"{len(shown)} rows vs live {len(live)}"]
    if extra:
        ex = set(extra)
        tags = []
        if ex & set(map(key, other)):
            tags.append("+OTHER JOB")
        if ex & set(map(key, earlier)):
            tags.append("+earlier runs")
        if ex - set(map(key, other)) - set(map(key, earlier)):
            tags.append("+unexplained")
        parts.append(f"extra {sum(extra.values())} {' '.join(tags)}")
    if missing:
        parts.append(f"missing {sum(missing.values())}")
    return "; ".join(parts)


def main() -> None:
    workdir = Path(sys.argv[1])
    expected = json.loads((workdir / "expected.json").read_text())
    for out in sys.argv[2:]:
        rec = json.loads(Path(out).read_text())
        print(
            f"\n## {out}: load={rec['load'][:200]} pods_importable={rec.get('pods_importable')}"
            f" pods_imported_by_load={rec.get('pods_imported_by_loaded_job')}"
        )
        if rec["load"] != "ok":
            continue
        jobname = Path(rec["job"]).stem.removesuffix("_job")
        live_nodes = expected[jobname]
        if isinstance(live_nodes, str):
            print("live job failed:", live_nodes)
            continue
        summ = rec["load_only_summary"]
        print("load_only summary:", summ if isinstance(summ, str) else "")
        print(
            "| node | type | as_table before | as_source | after load_only: as_table | node.run(load_only) | REPLAY as_source | load-only counts |"
        )
        print("|---|---|---|---|---|---|---|---|")
        for label in sorted(rec["after"]):
            live = live_nodes.get(label, {}).get("rows", [])
            if isinstance(live, str):
                live = []
            earlier = []
            if jobname == "main":
                for g in ("1", "2"):
                    earlier += expected["generations"][g].get(label, [])
            # Rows of the job sharing the tables: other for main, main for other.
            peer = {"main": "other", "other": "main"}.get(jobname)
            other = (
                [] if peer is None else expected[peer].get(label, {}).get("rows", [])
            )
            other = other if isinstance(other, list) else []
            b, a = rec["before"][label], rec["after"][label]
            nr = rec["node_run_load_only"].get(label)
            nr_rows = nr["rows"] if isinstance(nr, dict) else nr
            rp = rec["operator_replay_as_source"].get(label, "")
            counts = ""
            if isinstance(summ, dict) and label in summ["nodes"]:
                n = summ["nodes"][label]
                counts = f"reused {n['reused']} not_stored {n['not_stored']} computed {n['computed']}"
            print(
                f"| {label} | {a['type']} | {compare(b['as_table'], live, earlier, other)} "
                f"| {compare(b['as_source'], live, earlier, other)} "
                f"| {compare(a['as_table'], live, earlier, other)} "
                f"| {compare(nr_rows, live, earlier, other)} "
                f"| {compare(rp, live, earlier, other) if rp != '' else ''} | {counts} |"
            )


if __name__ == "__main__":
    main()
