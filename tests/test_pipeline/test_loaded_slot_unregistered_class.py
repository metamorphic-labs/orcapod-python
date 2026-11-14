"""A job loaded before its user class is registered reads like the job that saved it (ITL-1138).

A job over a column of a user class is saved; a fresh process loads it before anything has
registered the class (no source of it built, no function pod naming it defined). The saved type
of such a column cannot be restored to a class at load, so the loaded source slot keeps it as
pending text. The slot keeps its saved pipeline hash and content hash, and restores the type when
a source is bound, by which time the source has registered the class. Every node then has the
saving process's hashes, table schema (with its system-tag column names), rows and stored rows.
A slot whose class is still not registered at bind refuses the source with an error that names
the column and the saved class.

Each process runs in a fresh interpreter: what a class's registration changes is per process.
"""

import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

_CLASSES = textwrap.dedent(
    """\
    import dataclasses

    import orcapod
    import pydantic


    @dataclasses.dataclass
    class Exact:
        u: int
        v: str


    @dataclasses.dataclass
    class OtherExact:
        u: int
        v: str


    @orcapod.dataclass
    class Structural:
        u: int
        v: str


    class Model(pydantic.BaseModel):
        u: int
        v: str
    """
)

_SCRIPT = textwrap.dedent(
    """\
    import json
    import sys
    import warnings

    warnings.simplefilter("ignore")
    sys.path.insert(0, sys.argv[1])
    mode, work, class_name = sys.argv[2], sys.argv[3], sys.argv[4]

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.operators import Join, MapData
    from orcapod.sources import DictSource


    def sources(cls):
        s1 = DictSource(
            [{"id": 1, "p": cls(u=1, v="a")}, {"id": 2, "p": cls(u=2, v="b")}],
            tag_columns=["id"],
            data_schema={"id": int, "p": cls},
            source_id="s1",
        )
        s2 = DictSource([{"id": 1, "r": 5}, {"id": 2, "r": 6}], tag_columns=["id"], source_id="s2")
        return s1, s2


    def describe(job):
        out = {}
        for label, node in sorted(job.nodes.items()):
            table = node.as_table(all_info=True)
            # A source node stores nothing; a function or operator node has a stored view.
            stored_view = getattr(node, "as_source", None)
            stored = None if stored_view is None else stored_view().as_table(all_info=True)
            out[label] = {
                "pipeline_hash": node.pipeline_hash().to_string(),
                "content_hash": node.content_hash().to_string(),
                "schema": [f"{field.name}: {field.type}" for field in table.schema],
                "rows": table.num_rows,
                "stored_rows": None if stored is None else stored.num_rows,
            }
        return out


    if mode == "save":
        import classes

        cls = getattr(classes, class_name)
        s1, s2 = sources(cls)

        @function_pod(output_keys="q")
        def add(p: cls, r: int) -> int:
            return p.u + r

        @function_pod(output_keys="w")
        def read_u(pp: cls) -> int:
            return pp.u

        job = PipelineJob(name="demo", database=DeltaTableDatabase(work + "/store"))
        with job:
            joined = Join()(s1, s2, label="joined")
            add.pod(joined, label="add")
            renamed = MapData({"p": "pp"})(s1, label="renamed")
            read_u.pod(renamed, label="read_u")
        job.run()
        job.save(work + "/job.json")
        print(json.dumps(describe(job)))
    else:
        # Nothing registers the class before the load.
        job = PipelineJob.load(work + "/job.json")
        import classes

        bound_class = sys.argv[5] if len(sys.argv) > 5 else class_name
        s1, s2 = sources(getattr(classes, bound_class))  # registers the class now
        try:
            job.bind(sources={"s1": s1, "s2": s2})
        except Exception as exc:
            print(json.dumps({"bind_error": f"{type(exc).__name__}: {exc}"}))
            sys.exit(0)
        job.run(load_only=True)
        out = describe(job)
        out["_reused"] = {label: n.reused for label, n in job.last_run.nodes.items()}
        print(json.dumps(out))
    """
)


def _run(tmp_path: Path, *args: str) -> dict:
    """Run the script in a fresh interpreter and return the JSON it prints last."""
    result = subprocess.run(
        [sys.executable, str(tmp_path / "script.py"), str(tmp_path), *args],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.fixture
def saved_job(tmp_path: Path):
    """Return a function that saves the job over a class and returns the saving view."""
    _ = (tmp_path / "classes.py").write_text(_CLASSES)
    _ = (tmp_path / "script.py").write_text(_SCRIPT)

    def save(class_name: str) -> dict:
        return _run(tmp_path, "save", str(tmp_path / "work"), class_name)

    return save


@pytest.mark.parametrize("class_name", ["Exact", "Structural", "Model"])
def test_a_job_loaded_before_its_class_is_registered_reads_like_the_saved_job(
    tmp_path: Path, saved_job, class_name: str
) -> None:
    """Hashes, schemas with system-tag names, rows and stored rows equal the saving process."""
    saved = saved_job(class_name)
    loaded = _run(tmp_path, "load", str(tmp_path / "work"), class_name)
    assert "bind_error" not in loaded, loaded.get("bind_error")
    assert loaded.pop("_reused") == {"add": 2, "read_u": 2}
    assert loaded == saved


def test_a_slot_whose_class_is_never_registered_refuses_the_source_clearly(
    tmp_path: Path, saved_job
) -> None:
    """Binding a source of a look-alike class names the column and the saved class."""
    _ = saved_job("Exact")
    loaded = _run(tmp_path, "load", str(tmp_path / "work"), "Exact", "OtherExact")
    error = loaded["bind_error"]
    assert error.startswith("SourceSpecMismatchError: ")
    assert "'p'" in error
    assert "classes.Exact" in error
    assert "not registered" in error
