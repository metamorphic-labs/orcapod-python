"""Produces ``saved_job_37716c76``: a job saved by Orcapod v0.3 (format 0.2.0).

Not a test and not run in CI. It was run against the source of origin/main at commit
37716c76 (v0.3 after ITL-1154, before data-tags) from an empty folder:

    git archive 37716c76 src | tar -x -C <dir>
    PYTHONPATH=<dir>/src uv run python make_saved_job_37716c76.py <out dir>

A ``CSVSource`` with tag ``subject`` feeds one function node, ``older``, run on a
Delta store. Running it on a later checkout writes a newer format; do not regenerate
the fixture. It replaces ``saved_job_f51ce67c`` (format 0.1.0), whose hashes predate
the v1 hash definition (ITL-1154) and so cannot be reproduced.
"""

import os
import sys
from pathlib import Path

from orcapod import function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import PipelineJob
from orcapod.sources import CSVSource

out = Path(sys.argv[1])
out.mkdir(parents=True)
os.chdir(out)
Path("subjects.csv").write_text("subject,age\ns1,3\ns2,5\n")


@function_pod(output_keys="older")
def older(age: int) -> int:
    return age + 1


with PipelineJob(name="pipeline", database=DeltaTableDatabase("store")) as job:
    src = CSVSource(
        "subjects.csv", tag_columns=["subject"], source_id="subjects", label="subjects"
    )
    older.pod(src, label="older")
job.run()
job.save("job.json")
