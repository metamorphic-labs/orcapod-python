"""ITL-1403: compare interop dumps: same tables, schemas and rows (run-dependent columns left out).

usage: python compare.py A.json B.json [C.json ...]   (exit 1 when any differs from A)
"""

from __future__ import annotations

import json
import sys
from typing import Any

# Values a run makes, not the data: write times, run ids, row ids drawn at random.
VOLATILE = {"__pod_ts", "executed_at", "pipeline_run_id", "__data_id"}


def rows(name: str, table: dict[str, Any]) -> list[str]:
    out = []
    for row in table["rows"]:
        values = {k: v for k, v in json.loads(row).items() if k not in VOLATILE}
        text = json.dumps(values, sort_keys=True)
        if "/_result/" in name:
            # A result row's record id is a uuid7 drawn when it is stored; the
            # source-info tokens of its outputs repeat it.
            text = text.replace(values["__record_id"], "<record id>")
        out.append(text)
    return sorted(out)


def load(path: str) -> dict[str, Any]:
    data = json.load(open(path))
    # Table paths hold no version; compare them as they are.
    return data


def main() -> int:
    first, *others = sys.argv[1:]
    a = load(first)
    status = 0
    for other in others:
        before = status
        status = 0
        b = load(other)
        if set(a["tables"]) != set(b["tables"]):
            print(
                f"{other}: tables differ: {sorted(set(a['tables']) ^ set(b['tables']))}"
            )
            status = 1
            continue
        for name in sorted(a["tables"]):
            x, y = a["tables"][name], b["tables"][name]
            if x["schema"] != y["schema"]:
                print(
                    f"{other}: {name}: schema differs\n  {x['schema']}\n  {y['schema']}"
                )
                status = 1
            if rows(name, x) != rows(name, y):
                print(
                    f"{other}: {name}: rows differ ({len(x['rows'])} vs {len(y['rows'])})"
                )
                status = 1
        print(
            f"{first} vs {other}: {'equal' if status == 0 else 'DIFFERENT'} ({len(a['tables'])} tables)"
        )
        status = max(status, before)
    return status


if __name__ == "__main__":
    sys.exit(main())
