"""ITL-1179: run the golden tool with Orcapod's own-table float options forced off or on.

Step A of the final round runs ``--check`` and ``--stdout`` with the options off (what main
computes); step B runs ``--check`` with them on (as shipped). The tool reads and checks the
frozen and the provisional golden files alike. Under starfix 0.5.0, which has no float
options, ``_starfix_compat.py`` drops the keywords, so the same command runs on both
versions::

    uv run python docs/dev/scripts/itl_1179/golden_with_options.py off --check
    uv run python docs/dev/scripts/itl_1179/golden_with_options.py off --stdout --allow-errors
    uv run --with "starfix==0.5.0" python docs/dev/scripts/itl_1179/golden_with_options.py \\
        off --stdout --allow-errors
    uv run python docs/dev/scripts/itl_1179/golden_with_options.py on --check

The first argument is ``off`` or ``on``; the rest go to ``generate_golden_hashes.py``. Only
``--check`` and ``--stdout`` are meant here: the golden files are written by the tool itself,
as shipped (``on``), never through this script.
"""

from __future__ import annotations

import importlib.util
import sys
import warnings
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
warnings.filterwarnings("ignore")


def main(argv: list[str]) -> int:
    if not argv or argv[0] not in ("off", "on"):
        print("usage: golden_with_options.py off|on <generate_golden_hashes args>")
        return 2
    if not set(argv[1:]) & {"--check", "--stdout"}:
        print("golden_with_options.py runs only --check or --stdout")
        return 2
    spec = importlib.util.spec_from_file_location(
        "_starfix_compat", Path(__file__).with_name("_starfix_compat.py")
    )
    assert spec is not None and spec.loader is not None
    compat = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(compat)
    version = compat.use_installed_starfix()

    from orcapod.hashing import starfix_digest

    on = argv[0] == "on"
    starfix_digest.ORCAPOD_TABLE_FLOAT_OPTIONS = starfix_digest.FloatOptions(
        normalize_negative_zero=on, normalize_nan=on
    )
    print(f"# starfix {version}, own-table float options {argv[0]}", file=sys.stderr)

    from tests.test_hashing.golden import generate_golden_hashes

    generate_golden_hashes.main(argv[1:])
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
