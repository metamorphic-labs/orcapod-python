"""ITL-1472 evidence: a pytest plugin that applies the edge prototype to a test run.

    PYTHONPATH=docs/dev/scripts/itl_1472 uv run pytest -p prototype_plugin \
        tests/test_hashing/golden/test_golden_hashes.py -q

Every value is then computed with restrictions held on edges (``prototype.install``).
A golden value that moved would fail its golden test.
"""

import prototype

prototype.install()
