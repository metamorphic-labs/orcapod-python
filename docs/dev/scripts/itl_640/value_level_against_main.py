"""Value hashes of tables holding File and Directory columns (ITL-640, option (a)).

Option (a) changes the schema hash only; ``hash_table`` and every value hash built on it
must stay exactly as they are. This script prints, as JSON, the value-level hashes of a
fixed set of tables: ``hash_table`` (live and metadata form), each row's data hash, and
the default source id. Run it twice in one interpreter, once over this branch's source
and once over main's, and compare the outputs:

    uv run python docs/dev/scripts/itl_640/value_level_against_main.py OUT_DIR > branch.json
    PYTHONPATH=<main checkout>/src uv run python \\
        docs/dev/scripts/itl_640/value_level_against_main.py OUT_DIR > main.json

The fixture files and directories are written into ``OUT_DIR/fixtures``, which each run
deletes and creates again, at the same absolute path, so both runs hash the same paths
(the metadata form hashes the path text) and the same contents, and nothing else already
in ``OUT_DIR`` is touched or hashed.
"""

from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

import numpy as np

import orcapod as op
from orcapod.contexts import get_default_context
from orcapod.sources import ArrowTableSource
from orcapod.utils.arrow_utils import normalize_extension_columns

warnings.simplefilter("ignore")


def main(root: Path) -> dict[str, object]:
    """Write the fixtures under ``root`` and return the value-level hashes.

    Args:
        root: The output directory. Its ``fixtures`` folder is deleted and written
            again on every run, so each run hashes exactly the same files at the same
            absolute paths; nothing else in ``root`` is read or changed.

    Returns:
        The hashes of each case, by case and kind of hash.
    """
    import shutil

    root = root / "fixtures"
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    converter = get_default_context().type_converter
    hasher = get_default_context().arrow_hasher
    files, directories = [], []
    for index in range(4):
        # Paths 0 and 2 share a content, as do 1 and 3.
        path = root / f"f{index}.txt"
        path.write_text(f"content {index % 2}")
        files.append(op.File(path))
        folder = root / f"d{index}"
        folder.mkdir(exist_ok=True)
        (folder / "x.txt").write_text(f"content {index % 2}")
        directories.append(op.Directory(folder))
    cases = {
        "File": (op.File, files),
        "Directory": (op.Directory, directories),
        "list[File]": (list[op.File], [files[:2], files[2:], [files[1]], []]),
        "numpy.ndarray": (
            np.ndarray,
            [np.arange(i + 1, dtype=np.int16) for i in range(4)],
        ),
    }
    out: dict[str, object] = {}
    for label, (python_type, values) in cases.items():
        rows = [
            {"k": index, "t": values[index], "v": values[(index + 1) % 4]}
            for index in range(4)
        ]
        table = converter.python_dicts_to_arrow_table(
            rows, python_schema={"k": int, "t": python_type, "v": python_type}
        )
        metadata_form = normalize_extension_columns(table)
        source = ArrowTableSource(table, tag_columns=["k", "t"], tag_only_columns=True)
        out[label] = {
            "hash_table": hasher.hash_table(table).to_string(),
            "hash_table metadata form": hasher.hash_table(metadata_form).to_string(),
            "default source id": source.source_id,
            "row data hashes": [
                data.content_hash().to_string() for _, data in source.iter_data()
            ],
            "row tag hashes": [
                tag.content_hash().to_string() for tag, _ in source.iter_data()
            ],
            "paths 0 and 2 one table hash": label != "File"
            or hasher.hash_table(table.slice(0, 1).select(["v"]))
            == hasher.hash_table(table.slice(2, 1).select(["v"])),
        }
    return out


if __name__ == "__main__":
    print(json.dumps(main(Path(sys.argv[1])), indent=1, sort_keys=True))
