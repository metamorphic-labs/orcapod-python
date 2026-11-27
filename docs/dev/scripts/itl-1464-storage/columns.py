"""Summarize column families as rounded, normalized storage costs."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import TypedDict, cast

Families = dict[str, list[int]]
PREFIX_SETTINGS = "zstd-3-prefix-131072"


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


def column_family(table: str, column: str) -> str | None:
    """Group internal columns that carry the same kind of storage overhead."""
    if column == "__input_data_hash":
        return "Input data hash"
    if column == "__output_data_hash":
        return "Pipeline output data hash"
    if table == "pdb_v1" and column in {"__record_id", "__pipeline_base_entry_id"}:
        return "Pipeline record and base entry IDs"
    if column in {"__record_id", "__data_id"}:
        return "Result record and pipeline data IDs"
    if column.startswith("_tag_"):
        return "Pipeline system tags"
    if column.startswith("_source_"):
        return (
            "Input provenance tokens"
            if table == "pdb_v1"
            else "Output provenance tokens"
        )
    return None


def summarize(path: Path) -> dict[str, list[str]]:
    """Normalize private measurements by the pipeline row count."""
    tables = cast("dict[str, TableCounts]", json.loads(path.read_text()))
    rows = tables["pdb_v1"]["actual"]["rows"]
    sizes: Families = {}
    for name, table in tables.items():
        for index, column in enumerate(table["columns"]):
            family = column_family(name, column)
            if family is None:
                continue
            key = str(index)
            summed = sizes.setdefault(family, [0, 0, 0])
            summed[0] += table["arrow"][key]
            summed[1] += table["actual"]["compressed"].get(key, 0)
            summed[2] += table["computed"][PREFIX_SETTINGS]["compressed"].get(key, 0)
    return {
        family: [format(float(format(size / rows, ".2g")), "g") for size in variants]
        for family, variants in sizes.items()
    }


def main() -> None:
    """Write only rounded ratios, without source labels or absolute counts."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("narrow", type=Path, help="local metrics for a narrow result")
    parser.add_argument("wide", type=Path, help="local metrics for a wide result")
    args = parser.parse_args()
    narrow, wide = summarize(args.narrow), summarize(args.wide)
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(
        ["column_family"]
        + [
            f"{width}_{variant}_bytes_per_pipeline_row"
            for width in ("narrow", "wide")
            for variant in ("arrow", "delta", "zstd_prefix")
        ]
    )
    for family in narrow:
        writer.writerow([family, *narrow[family], *wide[family]])


if __name__ == "__main__":
    main()
