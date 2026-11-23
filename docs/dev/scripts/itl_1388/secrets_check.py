"""Does a saved job hold a database's credentials? (ITL-1388 question 1)

Usage: ``python secrets_check.py <workdir>``. A Delta database given ``storage_options``
holding a synthetic value (not a real credential) is attached to a job, which is saved;
the script prints only whether that value appears in the file, never the value.
PostgreSQL is not run here (no server): its connector's ``to_config`` returns the DSN
as given (``src/orcapod/databases/postgresql_connector.py``), password included.
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

warnings.simplefilter("ignore")

from orcapod import PipelineJob, function_pod  # noqa: E402
from orcapod.databases import DeltaTableDatabase  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402

work = Path(sys.argv[1])
work.mkdir(parents=True, exist_ok=True)
SYNTHETIC = "synthetic-value-" + "0" * 8  # stands in for a credential


@function_pod(output_keys="y")
def inc(x: int) -> int:
    return x + 1


db = DeltaTableDatabase(
    work / "store", storage_options={"aws_secret_access_key": SYNTHETIC}
)
job = PipelineJob(name="secrets", database=db)
with job:
    inc.pod(
        DictSource(
            data=[{"k": "a", "x": 1}],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="s",
        ),
        label="inc",
    )
job.save(work / "secrets_job.json")
text = (work / "secrets_job.json").read_text()
print("storage option value written into the job file:", SYNTHETIC in text)
print(
    "absolute database path written into the job file:",
    str((work / "store").resolve()) in text or str(work / "store") in text,
)
