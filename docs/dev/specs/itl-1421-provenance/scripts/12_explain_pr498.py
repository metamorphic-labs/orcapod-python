"""What ``stream.explain()`` (ITL-1402, PR 498, not on main yet) reports today.

Run against PR 498's source tree, for example:
    git archive origin/eywalker/itl-1382-filter-documentation-with-tested-samples src \
        | tar -x -C <dir>
    cd docs/dev/specs/itl-1421-provenance
    PYTHONPATH=<dir>/src uv run python -m scripts.12_explain_pr498

Proves: ``explain()`` names, per source a stream reads, what the source applies itself
(``where``, ``limit``) and which steps above it drop rows (a filter's condition, a join's
"rows without a match are dropped"). It is a statement about the PIPELINE, never about a
row: it does not say which records a step removed. Scenario 6 needs both.
"""

from __future__ import annotations

import polars as pl
from ._common import sessions_source, units_source

import orcapod

units = units_source()
sessions = sessions_source()
stream = (
    units.filter({"subject": "m1"})
    .join(sessions.polars_filter(pl.col("rig") != "C"))
    .drop_nulls()
    .group_by(["subject"])
)
print("orcapod from:", orcapod.__file__)
explanation = stream.explain()  # pyright: ignore[reportAttributeAccessIssue]  # explain() is in PR 498, not on main
print(type(explanation).__name__)
print(explanation.text())
