"""ITL-926 live verification of ``SpiralDBTableSource`` against a real SpiralDB project.

Run after ``spiral login`` (or with ``SPIRAL_WORKLOAD_ID`` set in CI), from the repository
root, with the ``spiraldb`` extra installed (``uv sync --all-extras``)::

    uv run python superpowers/specs/itl-926-live/live_verify.py \
        --project <project-id> [--dataset default] [--server-url http://api.spiraldb.dev] \
        [--keep] [--report report.json]

What it does, in scratch tables named ``itl926_<run>_*`` in ``--dataset`` (all dropped at the
end unless ``--keep``):

1. **Storage behaviour.** Writes one table per awkward column type straight through pyspiral
   (the way a workshop table is written: not through Orcapod) and records, per column, the
   type and nullability that were written, that ``Table.schema().to_arrow()`` reports, that a
   scan returns, and what ``SpiralDBTableSource`` exposes (Arrow field and Python type). This
   settles the behaviours that the test fake (``tests/test_databases/fake_pyspiral.py``)
   only assumes: value-column nullability, nested string types, binary type, dictionary
   decoding, decimal, timezone, extension types, struct-level nulls.
2. **Workshop path.** ``SpiralDBTableSource`` (composite key) -> ``PipelineJob`` with ``Join``
   against a ``DictSource`` and a function pod -> ``DeltaTableDatabase`` -> second run must
   make no function calls -> ``save`` -> ``PipelineJob.load`` in a fresh process ->
   ``get_all_records()`` of the function node, and the source's ``content_hash`` /
   ``pipeline_hash`` must equal the first process's.
3. **Row insert.** Inserts a row whose key sorts first and runs again: exactly one function
   call and four result rows are expected. Because record IDs are positional by default, the
   insert re-keys the rows after it; the report records ``get_all_records()``'s row count.

Nothing here needs the developer's interaction once logged in. Exit status is 0 when every
check passed, 1 otherwise; the JSON report has the details.
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import json
import os
import subprocess
import sys
import tempfile
import traceback
import uuid
from pathlib import Path
from typing import Any

import pyarrow as pa


def _type_cases() -> dict[str, tuple[pa.Array[Any], bool]]:
    """Column cases: name -> (values, written non-null)."""
    utc = dt.timezone.utc
    return {
        "string": (pa.array(["a", None, "c"], pa.string()), False),
        "large_string_nonnull": (pa.array(["a", "b", "c"], pa.large_string()), True),
        "binary": (pa.array([b"a", None, b"c"], pa.binary()), False),
        "large_binary_nonnull": (pa.array([b"a", b"b", b"c"], pa.large_binary()), True),
        "int8_nonnull": (pa.array([1, 2, 3], pa.int8()), True),
        "float32": (pa.array([1.5, None, 3.0], pa.float32()), False),
        "float64_nonnull": (pa.array([1.5, 2.5, 3.0], pa.float64()), True),
        "list_string": (pa.array([["a"], [], ["b", "c"]], pa.list_(pa.string())), True),
        "large_list_int32": (
            pa.array([[1], [2, 3], []], pa.large_list(pa.int32())),
            True,
        ),
        "list_nonnull_item": (
            pa.array([[1], [2], []], pa.list_(pa.field("item", pa.int64(), False))),
            True,
        ),
        "struct": (
            pa.array(
                [{"x": 1, "s": "a"}, {"x": 2, "s": None}, {"x": 3, "s": "c"}],
                pa.struct(
                    [pa.field("x", pa.int16(), False), pa.field("s", pa.string())]
                ),
            ),
            True,
        ),
        "dictionary": (pa.array(["u", "v", "u"]).dictionary_encode(), True),
        "timestamp_utc": (
            pa.array(
                [dt.datetime(2026, 11, 17, 9, tzinfo=utc)] * 3,
                pa.timestamp("us", "UTC"),
            ),
            True,
        ),
        "timestamp_naive_ms": (
            pa.array([dt.datetime(2026, 11, 17, 9)] * 3, pa.timestamp("ms")),
            True,
        ),
        "date32": (pa.array([dt.date(2026, 11, 17)] * 3, pa.date32()), True),
        "decimal": (
            pa.array(
                [decimal.Decimal("1.23"), None, decimal.Decimal("4.56")],
                pa.decimal128(10, 2),
            ),
            False,
        ),
        "bool": (pa.array([True, False, None], pa.bool_()), False),
    }


def _field_desc(f: pa.Field[Any]) -> str:
    return f"{f.type}{'' if f.nullable else ' not null'}"


class Run:
    def __init__(self, args: argparse.Namespace) -> None:
        import spiral as sp

        self.args = args
        self.overrides = {"server.url": args.server_url} if args.server_url else None
        self.sp = sp.Spiral(overrides=self.overrides)
        self.project = self.sp.project(args.project)
        self.prefix = f"itl926_{args.run_id}"
        self.created: list[str] = []
        self.report: dict[str, Any] = {"run_id": args.run_id, "checks": {}, "types": {}}

    def ident(self, name: str) -> str:
        return f"{self.args.dataset}.{self.prefix}_{name}"

    def create(self, name: str, key_schema: Any) -> Any:
        t = self.project.create_table(self.ident(name), key_schema=key_schema)
        self.created.append(self.ident(name))
        return t

    def check(self, name: str, ok: bool, detail: Any) -> None:
        self.report["checks"][name] = {"ok": bool(ok), "detail": detail}
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")

    # 1. storage behaviour ------------------------------------------------------------
    def storage_matrix(self) -> None:
        from orcapod.sources import SpiralDBTableSource

        for name, (values, nonnull) in _type_cases().items():
            entry: dict[str, Any] = {}
            self.report["types"][name] = entry
            try:
                schema = pa.schema(
                    [
                        pa.field("id", pa.int64(), False),
                        pa.field("v", values.type, not nonnull),
                    ]
                )
                table = pa.Table.from_arrays(
                    [pa.array(range(len(values)), pa.int64()), values], schema=schema
                )
                entry["written"] = _field_desc(schema.field("v"))
                t = self.create(f"t_{name}", [("id", pa.int64())])
                t.write(table)
                entry["table_schema"] = _field_desc(t.schema().to_arrow().field("v"))
                scanned = self.sp.scan(t.select()).to_table()
                entry["scan"] = _field_desc(scanned.schema.field("v"))
                entry["values_equal"] = (
                    scanned.column("v").to_pylist() == values.to_pylist()
                )
                src = SpiralDBTableSource(
                    self.args.project,
                    f"{self.prefix}_t_{name}",
                    dataset=self.args.dataset,
                    overrides=self.overrides,
                )
                entry["source_arrow"] = _field_desc(src.as_table().schema.field("v"))
                entry["source_python"] = str(src.output_schema()[1]["v"])
            except Exception as exc:  # noqa: BLE001 - every outcome is recorded
                entry["error"] = f"{type(exc).__name__}: {exc}"
            print(f"  {name:22} {entry}")

        # struct-level null: SpiralDB documents that it refuses these without push_down_nulls
        try:
            t = self.create("t_struct_null", [("id", pa.int64())])
            t.write(
                pa.table(
                    {
                        "id": pa.array([0, 1], pa.int64()),
                        "v": pa.array([{"x": 1}, None], pa.struct([("x", pa.int64())])),
                    }
                )
            )
            self.report["types"]["struct_null"] = {"write": "accepted"}
        except Exception as exc:  # noqa: BLE001
            self.report["types"]["struct_null"] = {
                "write": f"{type(exc).__name__}: {exc}"
            }

        # extension type written by Orcapod (Path logical type)
        try:
            from orcapod.sources import DictSource

            ext = (
                DictSource(
                    data=[{"id": 1, "p": Path("/a")}, {"id": 2, "p": Path("/b")}],
                    tag_columns=["id"],
                )
                .as_table()
                .select(["id", "p"])
            )
            entry = {"written": str(ext.schema.field("p").type)}
            t = self.create("t_extension", [("id", pa.int64())])
            t.write(ext)
            entry["scan"] = str(
                self.sp.scan(t.select()).to_table().schema.field("p").type
            )
            self.report["types"]["extension_path"] = entry
        except Exception as exc:  # noqa: BLE001
            self.report["types"]["extension_path"] = {
                "error": f"{type(exc).__name__}: {exc}"
            }

    # 2. workshop path ----------------------------------------------------------------
    def workshop_table(self) -> str:
        t = self.create(
            "recordings", [("subject_id", pa.string()), ("session", pa.int32())]
        )
        t.write(
            pa.table(
                {
                    "subject_id": pa.array(["m2", "m1", "m1"], pa.string()),
                    "session": pa.array([1, 2, 1], pa.int32()),
                    "firing_rate": pa.array([1.5, 2.5, 3.5], pa.float64()),
                    "notes": pa.array(["x", None, "z"], pa.string()),
                },
                schema=pa.schema(
                    [
                        pa.field("subject_id", pa.string(), False),
                        pa.field("session", pa.int32(), False),
                        pa.field("firing_rate", pa.float64(), False),
                        pa.field("notes", pa.string()),
                    ]
                ),
            )
        )
        return f"{self.prefix}_recordings"

    def workshop_path(self, workdir: Path) -> None:
        table_name = self.workshop_table()
        first = _workshop_job(self.args, table_name, workdir, save=True)
        self.report["workshop_first"] = first
        self.check(
            "run1 computes every joined row",
            first["run1_calls"] == 3,
            first["run1_calls"],
        )
        self.check(
            "run2 makes no function calls",
            first["run2_calls"] == 0,
            first["run2_calls"],
        )
        self.check(
            "source schema: firing_rate is non-null float",
            first["data_schema"].get("firing_rate") == "<class 'float'>",
            first["data_schema"],
        )

        cmd = [
            sys.executable,
            __file__,
            "--phase",
            "load",
            "--table",
            table_name,
            "--workdir",
            str(workdir),
            "--project",
            self.args.project,
            "--dataset",
            self.args.dataset,
            "--run-id",
            self.args.run_id,
        ]
        if self.args.server_url:
            cmd += ["--server-url", self.args.server_url]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        try:
            second = json.loads(proc.stdout.strip().splitlines()[-1])
        except Exception:  # noqa: BLE001
            second = {"error": proc.stdout[-2000:] + proc.stderr[-4000:]}
        self.report["workshop_load"] = second
        self.check(
            "fresh process: same content_hash",
            second.get("content_hash") == first["content_hash"],
            [first["content_hash"], second.get("content_hash")],
        )
        self.check(
            "fresh process: same pipeline_hash",
            second.get("pipeline_hash") == first["pipeline_hash"],
            [first["pipeline_hash"], second.get("pipeline_hash")],
        )
        self.check(
            "loaded job: get_all_records returns the 3 scores",
            second.get("score_rows") == 3,
            second,
        )

        # 3. row insert
        t = self.project.table(self.ident("recordings"))
        t.write(
            pa.table(
                {
                    "subject_id": pa.array(["m1"], pa.string()),
                    "session": pa.array([0], pa.int32()),
                    "firing_rate": pa.array([9.0], pa.float64()),
                    "notes": pa.array(["new"], pa.string()),
                }
            )
        )
        third = _workshop_job(self.args, table_name, workdir, save=False, runs=1)
        self.report["workshop_insert"] = third
        self.check(
            "insert one row: one function call",
            third["run1_calls"] == 1,
            third["run1_calls"],
        )
        self.check(
            "insert one row: the score node shows 4 rows",
            len(third["scores"]) == 4,
            third["scores"],
        )
        # Recorded, not checked: on main, get_all_records() after a source change returns
        # the previous run's rows (seen with the fake, outside ITL-926's scope).
        print("insert: get_all_records rows =", third["pipeline_records"])

    def cleanup(self) -> None:
        if self.args.keep:
            print("kept tables:", self.created)
            return
        for ident in reversed(self.created):
            try:
                self.project.drop_table(ident)
            except Exception as exc:  # noqa: BLE001
                print(f"could not drop {ident}: {exc}", file=sys.stderr)


def _workshop_job(
    args: argparse.Namespace,
    table_name: str,
    workdir: Path,
    *,
    save: bool,
    runs: int = 2,
) -> dict[str, Any]:
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.operators import Join
    from orcapod.sources import DictSource, SpiralDBTableSource

    overrides = {"server.url": args.server_url} if args.server_url else None
    calls: list[int] = []

    @function_pod(output_keys="score")
    def score(firing_rate: float, age: int, notes: str | None) -> float:
        calls.append(1)
        return firing_rate * age

    def build() -> tuple[Any, Any]:
        src = SpiralDBTableSource(
            args.project, table_name, dataset=args.dataset, overrides=overrides
        )
        subjects = DictSource(
            data=[{"subject_id": "m1", "age": 10}, {"subject_id": "m2", "age": 20}],
            tag_columns=["subject_id"],
            source_id="subjects",
        )
        job = PipelineJob(
            name="itl926_workshop", store=DeltaTableDatabase(workdir / "store")
        )
        with job:
            joined = Join()(src, subjects, label="joined")
            score.pod(joined, label="score")
        return src, job

    out: dict[str, Any] = {}
    src, job = build()
    out["content_hash"] = src.content_hash().to_string()
    out["pipeline_hash"] = src.pipeline_hash().to_string()
    out["tag_schema"] = {k: str(v) for k, v in src.output_schema()[0].items()}
    out["data_schema"] = {k: str(v) for k, v in src.output_schema()[1].items()}
    job.run()
    out["run1_calls"] = len(calls)
    out["scores"] = job.nodes["score"].as_table().to_pylist()
    records = job.nodes["score"].get_all_records()
    out["pipeline_records"] = None if records is None else records.num_rows
    if runs > 1:
        calls.clear()
        _, job2 = build()
        job2.run()
        out["run2_calls"] = len(calls)
        if save:
            job2.save(workdir / "job.json")
    return out


def _load_phase(args: argparse.Namespace) -> None:
    from orcapod import PipelineJob
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import SpiralDBTableSource

    workdir = Path(args.workdir)
    overrides = {"server.url": args.server_url} if args.server_url else None
    out: dict[str, Any] = {}
    src = SpiralDBTableSource(
        args.project, args.table, dataset=args.dataset, overrides=overrides
    )
    out["content_hash"] = src.content_hash().to_string()
    out["pipeline_hash"] = src.pipeline_hash().to_string()
    loaded = PipelineJob.load(
        workdir / "job.json", store=DeltaTableDatabase(workdir / "store")
    )
    out["unbound"] = list(loaded.unbound_sources)
    records = loaded.nodes["score"].get_all_records()
    out["score_rows"] = None if records is None else records.num_rows
    print(json.dumps(out))


def main() -> int:
    parser = argparse.ArgumentParser(description="ITL-926 live SpiralDB source check")
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", default="default")
    parser.add_argument("--server-url", default=os.environ.get("SPIRAL_SERVER_URL"))
    parser.add_argument(
        "--keep", action="store_true", help="do not drop the scratch tables"
    )
    parser.add_argument("--report", default=None, help="write the JSON report here")
    parser.add_argument("--phase", default="all", choices=["all", "load"])
    parser.add_argument("--run-id", default=uuid.uuid4().hex[:8])
    parser.add_argument("--table", default=None)
    parser.add_argument("--workdir", default=None)
    args = parser.parse_args()

    if args.phase == "load":
        _load_phase(args)
        return 0

    run = Run(args)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            try:
                run.storage_matrix()
            except Exception:  # noqa: BLE001
                run.check("storage matrix", False, traceback.format_exc())
            try:
                run.workshop_path(Path(tmp))
            except Exception:  # noqa: BLE001
                run.check("workshop path", False, traceback.format_exc())
    finally:
        run.cleanup()
    text = json.dumps(run.report, indent=2, default=str)
    if args.report:
        Path(args.report).write_text(text)
    print(text)
    return 0 if all(c["ok"] for c in run.report["checks"].values()) else 1


if __name__ == "__main__":
    sys.exit(main())
