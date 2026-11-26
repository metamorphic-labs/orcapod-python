"""ITL-1472 evidence: a pytest plugin that applies the prototype to a test run.

    PYTHONPATH=docs/dev/scripts/itl_1472 uv run pytest -p prototype_plugin \
        tests/test_hashing/golden/test_golden_hashes.py -q

Every golden value is then computed with invocations registered by their input edges
(``prototype.install``). A value that moved would fail its golden test.
"""

import prototype

prototype.install()
