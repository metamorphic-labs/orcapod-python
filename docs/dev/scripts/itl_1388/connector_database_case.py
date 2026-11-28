"""ITL-1388 (2): a job saved over a SQLite database cannot get its database back on load.

Usage: ``uv run python docs/dev/scripts/itl_1388/connector_database_case.py <workdir>``
"""

import json
import logging
import sys
from pathlib import Path

from orcapod import PipelineJob
from orcapod.databases import ConnectorArrowDatabase, SQLiteConnector
from orcapod.pipeline.serialization import DATABASE_REGISTRY
from orcapod.sources import DictSource

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
work = Path(sys.argv[1])
work.mkdir(parents=True, exist_ok=True)
db = ConnectorArrowDatabase(SQLiteConnector(work / "store.sqlite"))
job = PipelineJob(name="ops", database=db)
with job:
    src = DictSource(
        data=[{"k": "a", "x": 1}, {"k": "b", "x": 2}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
        record_id_columns=["k"],
    )
    src.drop_nulls(label="dropnulls")
job.run()
job.save(work / "job.json")
print(
    "1. save writes:",
    json.loads((work / "job.json").read_text())["bindings"]["database"],
)
print("2. database types load can rebuild:", sorted(DATABASE_REGISTRY))
loaded = PipelineJob.load(work / "job.json")  # logs the WARNING shown above this line
print("3. loaded job's database:", loaded.database)
loaded.save(work / "job_again.json")
print(
    "4. saved again:",
    json.loads((work / "job_again.json").read_text())["bindings"]["database"],
)
