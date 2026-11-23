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

Exit status 0 when equal, 3 when the comparison completed and found differences (every
difference is printed). Any other status (Python's 1 on an uncaught error) means the
comparison could not complete.
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa


def _read(path: Path) -> pa.Table:
    with pa.memory_map(str(path), "r") as source:
        return pa.ipc.open_file(source).read_all().combine_chunks()


# Columns whose value depends on the environment, not on the code under test: the git
# state of the file a function is defined in (``-dirty``, ``-untracked``), which changes
# whenever a file is added to the checkout during a run. Left out like volatile columns.
ENVIRONMENT_COLUMNS = frozenset({"__pf_var_git_hash"})


# The exit status of a completed comparison that found differences. It is distinct from
# Python's 1 for an uncaught error, so a caller can tell the two apart.
DIFFERENCES_FOUND = 3


def _events_by_node(events: list[Any]) -> dict[str, list[str]]:
    """Each node's events, in order; run-level events under ``""``.

    The order in which ``main`` starts independent nodes (two sources, say) varies from one
    process to the next, so only each node's own sequence of events is a result.
    """
    grouped: dict[str, list[str]] = {}
    for event in events:
        label = event[1] if len(event) > 1 else ""
        grouped.setdefault(label, []).append(json.dumps(event))
    return grouped


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


_FLOAT_BITS = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}


def _bits(array: Any) -> Any:
    """``array`` with every float as its integer bits, so equality is bit for bit.

    Arrow's ``equals`` holds NaN unequal to itself; two tables with the same NaN bits must
    compare equal, and ``0.0`` must stay distinct from ``-0.0``. Lists, fixed-size lists
    and structs are rebuilt over their converted children, keeping their validity.
    """
    kind = array.type
    if pa.types.is_floating(kind):
        return array.view(_FLOAT_BITS[kind.bit_width])
    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        cls = pa.LargeListArray if pa.types.is_large_list(kind) else pa.ListArray
        return cls.from_arrays(array.offsets, _bits(array.values), mask=array.is_null())
    if pa.types.is_fixed_size_list(kind):
        return pa.FixedSizeListArray.from_arrays(
            _bits(array.values), kind.list_size, mask=array.is_null()
        )
    if pa.types.is_struct(kind):
        children = [_bits(array.field(i)) for i in range(kind.num_fields)]
        return pa.StructArray.from_arrays(
            children,
            fields=[
                kind.field(i).with_type(child.type) for i, child in enumerate(children)
            ],
            mask=array.is_null(),
        )
    return array


def _bitwise(table: pa.Table) -> pa.Table:
    """``table`` with every column through ``_bits`` (names kept)."""
    columns = [_bits(column.combine_chunks()) for column in table.columns]
    return pa.Table.from_arrays(columns, names=table.column_names)


def _without(table: pa.Table, volatile: set[str]) -> pa.Table:
    return table.drop_columns([c for c in table.column_names if c in volatile])


def _rows_as_multiset(table: pa.Table) -> list[str]:
    """The rows as a sorted list of their serialized values, duplicates kept.

    Floats are taken by their bits (``_bitwise``), so NaN payloads stay distinct.
    """
    table = _bitwise(table)
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
    events_a: Any = _events_by_node(ma["events"])
    events_b: Any = _events_by_node(mb["events"])
    if mode == "set":
        events_a = sorted(json.dumps(e) for e in ma["events"])
        events_b = sorted(json.dumps(e) for e in mb["events"])
    if events_a != events_b:
        problems.append("observer events differ")
    for group in ("nodes", "stored"):
        only = sorted(set(ma[group]) ^ set(mb[group]))
        if only:
            problems.append(f"{group}: only in one dump: {only}")
    for name in sorted(set(ma["nodes"]) & set(mb["nodes"])):
        ea, eb = ma["nodes"][name].get("error"), mb["nodes"][name].get("error")
        if ea != eb:
            problems.append(f"nodes:{name}: as_table error {ea!r} != {eb!r}")
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
        volatile = set(volatile_by_table.get(key, [])) | ENVIRONMENT_COLUMNS
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
        if mode == "set":
            # Keep each volatile column's validity with its row, then drop its values.
            for name in sorted(volatile & set(x.column_names) & set(y.column_names)):
                x = x.append_column(f"__valid__{name}", x.column(name).is_valid())
                y = y.append_column(f"__valid__{name}", y.column(name).is_valid())
        xs, ys = _without(x, volatile), _without(y, volatile)
        if mode == "exact":
            if not _bitwise(xs).equals(_bitwise(ys)):
                problems.append(f"{key}: values differ")
        elif _rows_as_multiset(xs) != _rows_as_multiset(ys):
            problems.append(f"{key}: rows differ as sets")
    return problems


def _as_multiset(array: Any) -> collections.Counter[str]:
    """``array``'s values, bit for bit (``_bits``), counted without their positions."""
    return collections.Counter(repr(value) for value in _bits(array).to_pylist())


def calibrate_tables(
    ta: dict[str, Any], tb: dict[str, Any]
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """The volatile columns of two dumps of one checkout, and their row-order variation.

    A column is volatile only when its values differ as a multiset between the two
    dumps (a run-made identifier, a timestamp). A column whose values are the same
    multiset in another row order is not volatile: the row order varied, and later
    drift in its values must still be found, so it is reported apart (CodeRabbit on
    #510).

    Args:
        ta: The first dump's tables, by key.
        tb: The second dump's tables, by key.

    Returns:
        The volatile columns by table, and the columns whose rows came in another
        order by table.
    """
    volatile: dict[str, list[str]] = {}
    reordered: dict[str, list[str]] = {}
    for key in sorted(set(ta) & set(tb)):
        x, y = ta[key], tb[key]
        if x.num_rows != y.num_rows:
            continue
        names = [n for n in x.column_names if n in y.column_names]
        for n in names:
            cx, cy = x.column(n).combine_chunks(), y.column(n).combine_chunks()
            if _bits(cx).equals(_bits(cy)):
                continue
            if _as_multiset(cx) == _as_multiset(cy):
                reordered.setdefault(key, []).append(n)
            else:
                volatile.setdefault(key, []).append(n)
    return volatile, reordered


def calibrate(a: Path, b: Path) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """``calibrate_tables`` of two dumps of the same checkout."""
    return calibrate_tables(_tables(a), _tables(b))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("a", type=Path)
    parser.add_argument("b", type=Path)
    parser.add_argument("--mode", choices=("exact", "set"), default="exact")
    parser.add_argument("--volatile", type=Path)
    parser.add_argument("--calibrate", type=Path)
    args = parser.parse_args(argv)
    if args.calibrate is not None:
        found, reordered = calibrate(args.a, args.b)
        args.calibrate.write_text(json.dumps(found, indent=1, sort_keys=True))
        print(json.dumps(found, indent=1, sort_keys=True))
        for key, names in sorted(reordered.items()):
            # Not volatile: the values are the same, in another row order.
            print(f"row order varied in {key}: {names}", file=sys.stderr)
        return 0
    volatile: dict[str, Any] = (
        json.loads(args.volatile.read_text()) if args.volatile is not None else {}
    )
    problems = compare(args.a, args.b, args.mode, volatile)
    for line in problems:
        print(line)
    print("EQUAL" if not problems else f"{len(problems)} DIFFERENCES")
    return 0 if not problems else DIFFERENCES_FOUND


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
