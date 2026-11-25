"""Export anonymous per-column bytes, bytes per row, and file shares as CSV."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import TypedDict, cast


class ByteCounts(TypedDict):
    """Bytes in one physical Parquet variant."""

    bytes: int
    rows: int
    compressed: dict[str, int]


class TableCounts(TypedDict):
    """Measurement record for one Delta table."""

    columns: list[str]
    arrow: dict[str, int]
    actual: ByteCounts
    computed: dict[str, ByteCounts]
    sorted: dict[str, ByteCounts]


def main() -> None:
    """Write the column matrix to stdout without logging paths or values."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "cases", nargs="+", help="each input has form anonymous-label=metrics.json"
    )
    args = parser.parse_args()
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(
        [
            "case",
            "table",
            "order",
            "settings",
            "column",
            "rows",
            "arrow_bytes",
            "arrow_bytes_per_row",
            "parquet_bytes",
            "parquet_bytes_per_row",
            "fraction_of_parquet",
        ]
    )
    for case in cast("list[str]", args.cases):
        label, file = case.split("=", 1)
        metrics = cast("dict[str, TableCounts]", json.loads(Path(file).read_text()))
        for kind, table in metrics.items():
            variants = [("stored", "delta", table["actual"])]
            variants.extend(
                ("computed", key, value)
                for key, value in table.get("computed", {}).items()
            )
            variants.extend(
                ("tags", key, value) for key, value in table.get("sorted", {}).items()
            )
            for order, settings, measurement in variants:
                total = measurement["bytes"]
                rows = measurement["rows"]
                for index, name in enumerate(table["columns"]):
                    arrow = table["arrow"][str(index)]
                    parquet = measurement["compressed"].get(str(index), 0)
                    writer.writerow(
                        [
                            label,
                            kind,
                            order,
                            settings,
                            name,
                            rows,
                            arrow,
                            round(arrow / rows, 5),
                            parquet,
                            round(parquet / rows, 5),
                            round(parquet / total, 6),
                        ]
                    )
                overhead = total - sum(measurement["compressed"].values())
                writer.writerow(
                    [
                        label,
                        kind,
                        order,
                        settings,
                        "(file overhead)",
                        rows,
                        "",
                        "",
                        overhead,
                        round(overhead / rows, 5),
                        round(overhead / total, 6),
                    ]
                )


if __name__ == "__main__":
    main()
