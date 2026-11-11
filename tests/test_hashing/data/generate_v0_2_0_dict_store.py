"""Build a Delta result store with the released v0.2.0 code (ITL-905 test fixture).

Run from a checkout of the v0.2.0 tag, then pack the store:

    uv run python generate_v0_2_0_dict_store.py <store_dir>
    tar -czf v0_2_0_dict_store.tar.gz -C <store_dir> .

The process can hang at interpreter exit inside deltalake after printing; the store is
complete once the record count is printed.
"""

import sys
import orcapod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource


def total(m: dict[str, int]) -> int:
    return sum(m.values())


out = sys.argv[1]
db = DeltaTableDatabase(out)
pod = CachedFunctionPod(
    FunctionPod(PythonDataFunction(total, output_keys="total")), result_database=db
)
src = DictSource(
    data=[
        {"k": "unsorted", "m": {"b": 2, "a": 1}},
        {"k": "sorted", "m": {"a": 1, "c": 3}},
    ],
    tag_columns=["k"],
    data_schema={"k": str, "m": dict[str, int]},
    source_id="itl905_v020",
)
res = list(pod.process(src).iter_data())
db.flush()
for t, d in res:
    print(t.as_dict(), d.as_dict(), d.content_hash().to_string())
for t, d in src.iter_data():
    print("input", t.as_dict(), d.content_hash().to_string())
print(pod.record_path)
print(db.get_all_records(pod.record_path).num_rows)
print("version", orcapod.__version__ if hasattr(orcapod, "__version__") else "?")
