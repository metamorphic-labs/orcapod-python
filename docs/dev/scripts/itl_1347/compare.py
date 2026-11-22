"""ITL-1347 differential, the comparison: two dumps of ``dump.py``, exactly or as sets.

    uv run python compare.py <dump A> <dump B> --mode exact|set [--volatile volatile.json]
    uv run python compare.py <dump A> <dump B> --calibrate volatile.json

- ``exact`` (sync branch against sync pinned ``main``): every node table and stored table is
  equal byte for byte (``Table.equals(check_metadata=True)`` on combined chunks, schemas
  field by field), the row order included. The same holds for the run summary, the errors,
  the warnings, the observer events and the Delta version counts. Only the volatile columns
  are left out of the value comparison. Their schema and null pattern are still compared.
- ``set`` (async against sync): the same, except that rows are compared as a multiset, with
  columns matched by name; observer events are compared as a multiset too.
- ``--calibrate``: compares two dumps of the **same** checkout and writes the columns whose
  values differ between them (identifiers and timestamps a run makes, such as a ``uuid7``
  datagram id or a write time) to ``volatile.json``. Only those columns are ever left out.

Exit status 0 when equal; otherwise every difference is printed and the status is 1.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa


def _read(path: Path) -> pa.Table:
    with pa.memory_map(str(path), "r") as source:
        return pa.ipc.open_file(source).read_all().combine_chunks()


def _tables(dump: Path) -> dict[str, pa.Table]:
    manifest = json.loads((dump / "manifest.json").read_text())
    found: dict[str, pa.Table] = {}
    for group in ("nodes", "stored"):
        for name, entry in manifest[group].items():
            if "file" in entry:
                found[f"{group}:{name}"] = _read(dump / entry["file"])
    return found


def _schema_differences(key: str, a: pa.Table, b: pa.Table) -> list[str]:
    if a.schema.equals(b.schema, check_metadata=True):
        return []
    lines = [f"{key}: schemas differ"]
    for name in sorted(set(a.column_names) | set(b.column_names)):
        if name not in a.column_names or name not in b.column_names:
            lines.append(f"  column {name!r} only in one side")
            continue
        fa, fb = a.schema.field(name), b.schema.field(name)
        if not fa.equals(fb, check_metadata=True):
            lines.append(f"  {name!r}: {fa} != {fb}")
    if a.column_names != b.column_names:
        lines.append(f"  column order: {a.column_names} != {b.column_names}")
    return lines


def _without(table: pa.Table, volatile: set[str]) -> pa.Table:
    return table.drop_columns([c for c in table.column_names if c in volatile])


def _rows_as_multiset(table: pa.Table) -> list[str]:
    names = sorted(table.column_names)
    return sorted(
        json.dumps([repr(row[n]) for n in names])
        for row in table.select(names).to_pylist()
    )


def compare(
    a: Path, b: Path, mode: str, volatile_by_table: dict[str, list[str]]
) -> list[str]:
    """Every difference between dumps ``a`` and ``b``."""
    problems: list[str] = []
    ma = json.loads((a / "manifest.json").read_text())
    mb = json.loads((b / "manifest.json").read_text())
    for field in ("error", "summary", "warnings"):
        if ma[field] != mb[field]:
            problems.append(f"{field}: {ma[field]!r} != {mb[field]!r}")
    events_a, events_b = ma["events"], mb["events"]
    if mode == "set":
        events_a = sorted(json.dumps(e) for e in events_a)
        events_b = sorted(json.dumps(e) for e in events_b)
    if events_a != events_b:
        problems.append("observer events differ")
    for name in sorted(set(ma["stored"]) | set(mb["stored"])):
        va = ma["stored"].get(name, {}).get("versions")
        vb = mb["stored"].get(name, {}).get("versions")
        if va != vb:
            problems.append(f"stored:{name}: Delta versions {va} != {vb}")
    ta, tb = _tables(a), _tables(b)
    for key in sorted(set(ta) | set(tb)):
        if key not in ta or key not in tb:
            problems.append(f"{key}: only in one dump")
            continue
        x, y = ta[key], tb[key]
        volatile = set(volatile_by_table.get(key, []))
        if mode == "exact":
            problems += _schema_differences(key, x, y)
        else:
            x, y = x.select(sorted(x.column_names)), y.select(sorted(y.column_names))
            problems += _schema_differences(key, x, y)
        if x.num_rows != y.num_rows:
            problems.append(f"{key}: {x.num_rows} rows != {y.num_rows} rows")
            continue
        for name in volatile & set(x.column_names) & set(y.column_names):
            if mode == "exact" and not x.column(name).is_null().equals(
                y.column(name).is_null()
            ):
                problems.append(f"{key}: null pattern of volatile {name!r} differs")
        xs, ys = _without(x, volatile), _without(y, volatile)
        if mode == "exact":
            if not xs.equals(ys, check_metadata=True):
                problems.append(f"{key}: values differ")
        elif _rows_as_multiset(xs) != _rows_as_multiset(ys):
            problems.append(f"{key}: rows differ as sets")
    return problems


def calibrate(a: Path, b: Path) -> dict[str, list[str]]:
    """The columns whose values differ between two dumps of the same checkout."""
    volatile: dict[str, list[str]] = {}
    ta, tb = _tables(a), _tables(b)
    for key in sorted(set(ta) & set(tb)):
        x, y = ta[key], tb[key]
        if x.num_rows != y.num_rows:
            continue
        names = [n for n in x.column_names if n in y.column_names]
        differing = [n for n in names if not x.column(n).equals(y.column(n))]
        if differing:
            volatile[key] = differing
    return volatile


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("a", type=Path)
    parser.add_argument("b", type=Path)
    parser.add_argument("--mode", choices=("exact", "set"), default="exact")
    parser.add_argument("--volatile", type=Path)
    parser.add_argument("--calibrate", type=Path)
    args = parser.parse_args(argv)
    if args.calibrate is not None:
        found = calibrate(args.a, args.b)
        args.calibrate.write_text(json.dumps(found, indent=1, sort_keys=True))
        print(json.dumps(found, indent=1, sort_keys=True))
        return 0
    volatile: dict[str, Any] = (
        json.loads(args.volatile.read_text()) if args.volatile is not None else {}
    )
    problems = compare(args.a, args.b, args.mode, volatile)
    for line in problems:
        print(line)
    print("EQUAL" if not problems else f"{len(problems)} DIFFERENCES")
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
