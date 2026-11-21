"""Write, or check, the data-types-per-database page (ITL-1334).

usage: uv run python scripts/database_type_matrix.py [--check]

Measures every cell of the matrix (``tests/test_databases/type_matrix.py``: every type
written and read back through every database, in a temporary directory) and writes
``docs/reference/database-types.md``. With ``--check`` it writes nothing and exits 1 when
the committed page differs from the measurements.
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
import warnings
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> int:
    """Measure the matrix and write or check the page."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="compare, write nothing")
    args = parser.parse_args()

    from tests.test_databases.type_matrix import PAGE, measure_all, render

    warnings.simplefilter("ignore")
    logging.disable(logging.WARNING)
    with tempfile.TemporaryDirectory() as tmp:
        text = render(measure_all(Path(tmp)))
    if args.check:
        if PAGE.read_text(encoding="utf-8") != text:
            print(f"{PAGE} differs from the measurements; run without --check")
            return 1
        print(f"{PAGE} matches the measurements")
        return 0
    PAGE.write_text(text, encoding="utf-8")
    print(f"wrote {PAGE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
