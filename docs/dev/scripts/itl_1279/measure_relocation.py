"""ITL-1279 and ITL-1324: which golden entries the relocation would move (measurement only).

usage: uv run python docs/dev/scripts/itl_1279/measure_relocation.py [mixed]

``mixed``: per entry, for the cases that mix frozen and provisional entries.

In this process only, the approved provisional operators are marked provisional
(``PROVISIONAL = True`` and ``PROVISIONAL_OPERATORS``), and the use of a provisional
library value type (``provisional`` of ``contexts/logical_types.json``) is noted by its
extension name, as the change will do. Every golden case of every available group then
runs inside ``recording_provisional_uses``. For each case that uses a provisional feature
the script prints: the features, the number of entries, the golden file(s) pinning them
(frozen or provisional), and whether the case already declares them. Totals at the end.
No golden file is read for writing and nothing is written.
"""

from __future__ import annotations

import importlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "tests" / "test_hashing" / "golden"))

APPROVED = {
    "GroupBy": "group_by",
    "TagBy": "tag_by",
    "AddTags": "add_tags",
    "MergeJoin": "merge_join",
    "MapTags": "mappers",
    "MapColumns": "mappers",
    "Pick": "pick",
    "Index": "index",
    "Batch": "batch",
}


def install() -> None:
    from orcapod.core.operators import provisional as prov
    from orcapod.utils.provisional import note_provisional_use

    names = dict(prov.PROVISIONAL_OPERATORS)
    for name, module in APPROVED.items():
        cls = getattr(importlib.import_module(f"orcapod.core.operators.{module}"), name)
        cls.PROVISIONAL = True
        names.setdefault(name, "measuring")
    prov.PROVISIONAL_OPERATORS = names  # pyright: ignore[reportAttributeAccessIssue]

    import orcapod.hashing.semantic_hashing.type_handler_registry as handlers
    import orcapod.logical_types.registry as registry
    from orcapod.logical_types import provisional as lt_prov

    original = lt_prov.warn_provisional

    def noted(extension_name: str) -> None:
        note_provisional_use(extension_name)
        original(extension_name)

    lt_prov.warn_provisional = noted
    handlers.warn_provisional = noted  # pyright: ignore[reportAttributeAccessIssue]
    registry.warn_provisional = noted  # pyright: ignore[reportAttributeAccessIssue]


def pinned_files() -> dict[str, dict[str, str]]:
    """group -> entry -> the golden file (relative) that pins it."""
    folder = ROOT / "tests" / "test_hashing" / "golden" / "data" / "v1"
    where: dict[str, dict[str, str]] = defaultdict(dict)
    for path in sorted(folder.rglob("*.json")):
        relative = path.relative_to(folder).as_posix()
        group = path.stem.split("-")[0]
        for entry in json.loads(path.read_text()):
            where[group][entry] = relative
    return where


def main() -> None:
    import warnings

    warnings.simplefilter("ignore")
    install()
    import cases  # pyright: ignore[reportMissingImports]
    from orcapod.utils.provisional import recording_provisional_uses

    where = pinned_files()
    totals: Counter[str] = Counter()
    by_feature: Counter[str] = Counter()
    for group in cases.available_groups():
        for name, fn, declared in cases._CASES[group]:  # pyright: ignore[reportPrivateUsage]
            with recording_provisional_uses() as used:
                values = fn()
            used = set(used)
            entries = [f"{name}.{k}" if k else name for k in values]
            files = Counter(where[group].get(e, "UNPINNED") for e in entries)
            if not used:
                frozen_but_declared = bool(declared)
                if frozen_but_declared:
                    print(f"{group}:{name} declares {declared} but uses none")
                continue
            moving = sum(
                n for f, n in files.items() if not f.startswith("provisional/")
            )
            totals["cases"] += 1
            totals["entries"] += len(entries)
            totals["moving"] += moving
            for feature in used:
                by_feature[feature] += len(entries)
            print(
                f"{group}:{name}: uses {sorted(used)}; {len(entries)} entries; "
                f"files {dict(files)}; declared {list(declared)}; moving {moving}"
            )
    print()
    print("totals:", dict(totals))
    print(
        "entries per feature (a case counts for each feature it uses):",
        dict(by_feature),
    )


if __name__ == "__main__" and sys.argv[1:] != ["mixed"]:
    main()


def mixed() -> None:
    """Per entry, for the cases that mix frozen and provisional entries: which entries
    depend on a provisional feature (each item computed alone, while recording)."""
    import uuid
    import warnings

    import numpy as np
    import pandas as pd
    import pyarrow as pa

    warnings.simplefilter("ignore")
    install()
    import cases  # pyright: ignore[reportMissingImports]
    import orcapod as op
    from orcapod.types import Schema
    from orcapod.utils.provisional import recording_provisional_uses

    ctx = cases._ctx()  # pyright: ignore[reportPrivateUsage]
    cases.register_logical_classes()

    def attribute(label: str, items: dict[str, Any], compute: Any) -> None:
        moving, staying = [], []
        for key, item in items.items():
            with recording_provisional_uses() as used:
                compute(item)
            (moving if used else staying).append(key)
        print(f"{label}: {len(moving)} provisional entries {sorted(moving)}")
        print(f"{label}: {len(staying)} frozen entries")

    hasher = ctx.semantic_hasher
    attribute(
        "semantic:value (1 entry per key)", cases.SEMANTIC_VALUES, hasher.hash_object
    )
    attribute(
        "semantic:type (1 entry per key)",
        cases._semantic_types(),  # pyright: ignore[reportPrivateUsage]
        hasher.hash_object,
    )

    def single_field(tp: Any) -> None:
        ctx.type_converter.python_type_to_arrow_type(tp)
        hasher.hash_object(Schema({"c": tp}))

    attribute(
        "schema:single_field (2 entries per key)",
        cases._semantic_types(),  # pyright: ignore[reportPrivateUsage]
        single_field,
    )
    types = {
        "uuid": uuid.UUID,
        "file": op.File,
        "directory": op.Directory,
        "ndarray": np.ndarray,
        "dataframe": pd.DataFrame,
        "series": pd.Series,
        "list_of_file": list[op.File],
        "set_of_uuid": set[uuid.UUID],
    }

    def extension(python_type: Any) -> None:
        for rows in (0, 2):
            arrow_type = ctx.type_converter.python_type_to_arrow_type(python_type)
            storage = pa.array([None] * rows, arrow_type.storage_type)
            column = pa.ExtensionArray.from_storage(arrow_type, storage)
            ctx.arrow_hasher.hash_table(pa.table({"c": column}))

    attribute("arrow:extension_no_values (2 entries per key)", types, extension)


if __name__ == "__main__" and sys.argv[1:] == ["mixed"]:
    mixed()
