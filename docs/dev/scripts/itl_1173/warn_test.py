"""ITL-1173: does delta.dataSkippingStatsColumns stop the binary-field warnings on
Orcapod's own create / append / merge / schema-evolution paths, and for an
EXISTING table? Each scenario runs in a subprocess so stderr can be counted.

Usage: uv run python docs/dev/scripts/itl_1173/warn_test.py <scenario> <dir>     (driver: warn_test.py all <dir>)
"""

import os
import subprocess
import sys
from typing import Any

import pyarrow as pa


def patch_create(property_on: bool) -> None:
    """Make Orcapod's create path pass the statistics property (experiment only)."""
    if not property_on:
        return
    from orcapod.databases import delta_lake_databases as dldb

    orig = dldb.deltalake.write_deltalake

    def write(table_uri, data, *a, **k):
        if k.get("mode") == "overwrite":
            mode = os.environ.get("ITL1173_PROP", "stats_cols")
            if mode == "num0":
                k["configuration"] = {"delta.dataSkippingNumIndexedCols": "0"}
            else:

                def stat_ok(t):
                    t = t.storage_type if isinstance(t, pa.ExtensionType) else t
                    return (
                        pa.types.is_integer(t)
                        or pa.types.is_floating(t)
                        or pa.types.is_string(t)
                        or pa.types.is_large_string(t)
                        or pa.types.is_boolean(t)
                        or pa.types.is_date(t)
                        or pa.types.is_timestamp(t)
                        or pa.types.is_decimal(t)
                    )

                cols = [
                    "`" + f.name.replace("`", "``") + "`"
                    for f in data.schema
                    if stat_ok(f.type)
                ]
                k["configuration"] = {"delta.dataSkippingStatsColumns": ",".join(cols)}
        return orig(table_uri, data, *a, **k)

    dldb.deltalake.write_deltalake = write


def rows(ids, extra=False):
    cols: dict[str, pa.Array[Any]] = {
        "__record_id": pa.array([f"id{i}".encode() for i in ids], pa.large_binary()),
        "h": pa.array([f"h{i}".encode() for i in ids], pa.large_binary()),
        "v": pa.array(ids, pa.int64()),
    }
    if extra:
        cols["new_bin"] = pa.array([b"x"] * len(ids), pa.large_binary())
        cols["new_int"] = pa.array(ids, pa.int64())
    return pa.Table.from_arrays(list(cols.values()), names=list(cols))


def scenario(name: str, d: str) -> None:
    from orcapod.databases import DeltaTableDatabase

    prop = name.endswith("_prop")
    patch_create(prop)
    db = DeltaTableDatabase(d)
    p = ("t",)
    step = name.removesuffix("_prop")
    if step == "create":
        db.add_records(p, rows(range(0, 10)), flush=True)
    elif step == "append":
        db.add_records(p, rows(range(10, 20)), flush=True)
    elif step == "merge":
        # One stored id among the pending ones forces the MERGE path.
        db.add_records(p, rows(range(15, 30)), skip_duplicates=False, flush=True)
    elif step == "evolve":
        # A new column (one binary, one int): schema evolution goes through MERGE.
        db.add_records(p, rows(range(30, 40), extra=True), flush=True)
    elif step == "append_after_evolve":
        db.add_records(p, rows(range(40, 50), extra=True), flush=True)
    elif step == "append_new":
        db.add_records(p, rows(range(50, 60), extra=True), flush=True)
    elif step == "set_property_on_existing":
        import deltalake

        dt = deltalake.DeltaTable(os.path.join(d, "t"))
        cols = [
            f.name
            for f in pa.schema(dt.schema().to_arrow())  # pyright: ignore[reportArgumentType]  # an arro3 schema, read through the Arrow C interface
            if not (pa.types.is_binary(f.type) or pa.types.is_large_binary(f.type))
        ]
        dt.alter.set_table_properties(
            {"delta.dataSkippingStatsColumns": ",".join(cols)}
        )
    import deltalake

    dt = deltalake.DeltaTable(os.path.join(d, "t"))
    print(
        f"version={dt.version()} config={dt.metadata().configuration} rows={dt.to_pyarrow_table().num_rows}"
    )


def run(step: str, d: str) -> str:
    try:
        r = subprocess.run(
            [sys.executable, __file__, step, d],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        return f"{step:28s} TIMEOUT"
    warns = [ln for ln in r.stderr.splitlines() if "Skipping column" in ln]
    cols = sorted({ln.split("Skipping column ")[1].split(" ")[0] for ln in warns})
    return f"{step:28s} warnings={len(warns)} {cols} | {r.stdout.strip()} {r.stderr.strip()[-300:] if r.returncode else ''}"


if __name__ == "__main__":
    if sys.argv[1] == "all":
        base = sys.argv[2]
        for variant in ("", "_prop"):
            d = os.path.join(base, f"t{variant or '_none'}")
            print(f"--- table created {'WITH' if variant else 'WITHOUT'} the property")
            for step in ("create", "append", "merge", "evolve", "append_after_evolve"):
                print(run(step + variant, d))
        d = os.path.join(base, "t_none")
        print("--- existing table (created without): set the property, then append")
        print(run("set_property_on_existing", d))
        print(run("append_new", d))
    else:
        scenario(sys.argv[1], sys.argv[2])
        if os.environ.get("ITL1173_HARD_EXIT", "1") == "1":
            # Interpreter exit after a MERGE hangs in Arrow's ThreadPool shutdown
            # (seen with deltalake 1.0.2 / pyarrow 23.0.1); skip it here.
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(0)
