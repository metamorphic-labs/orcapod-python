"""TEMPORARY stress of child process exit (ITL-1422). Draft PR only, never merged."""

from __future__ import annotations

import collections
import importlib.util
import os
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNS = int(os.environ.get("ITL1422_RUNS", "300"))
PARALLEL = int(os.environ.get("ITL1422_PARALLEL", "8"))
DEADLINE = 60
LINUX = platform.system() == "Linux"

TRACER = r"""
#include <execinfo.h>
#include <signal.h>
#include <unistd.h>
#include <sys/syscall.h>
static void h(int s){ void* b[80]; int n=backtrace(b,80);
 dprintf(2,"NATIVE pid=%d tid=%ld\n",getpid(),(long)syscall(SYS_gettid));
 backtrace_symbols_fd(b,n,2); signal(s,SIG_DFL); raise(s);}
__attribute__((constructor)) static void i(void){ void* b[2]; backtrace(b,2);
 struct sigaction a={0}; a.sa_handler=h; sigaction(SIGABRT,&a,0);}
"""

# Pre-made pool threads run at SCHED_IDLE, so a task's clean-up trails its result.
AMPLIFY = textwrap.dedent(
    """
    def _demote():
        import os, threading
        if not hasattr(os, "SCHED_IDLE"):
            return
        me = threading.get_native_id()
        for t in os.listdir("/proc/self/task"):
            if int(t) != me:
                try:
                    os.sched_setscheduler(int(t), os.SCHED_IDLE, os.sched_param(0))
                except OSError:
                    pass
    def _warm():
        import os, tempfile, pyarrow as pa, pyarrow.parquet as pq, pyarrow.dataset as ds
        with tempfile.TemporaryDirectory() as d:
            for i in range(16):
                pq.write_table(pa.table({"a": list(range(5000))}), f"{d}/{i}.parquet")
            ds.dataset(d).to_table()
            ds.dataset(d).to_table()
        _demote()
    _warm()
    """
)

SERIAL = textwrap.dedent(
    """
    import orcapod.databases.delta_lake_databases as _m
    import orcapod.databases.storage_utils as _s
    import orcapod.core.sources.delta_table_source as _t
    _real = _s.delta_to_pyarrow_dataset
    class _D:
        def __init__(self, d): self._d = d
        def to_table(self, **k): return self._d.to_table(use_threads=False, **k)
        def __getattr__(self, n): return getattr(self._d, n)
    for _x in (_m, _t): _x.delta_to_pyarrow_dataset = lambda *a, **k: _D(_real(*a, **k))
    """
)

SINGLE = textwrap.dedent(
    """
    import ctypes, sys
    import orcapod.logical_types.registry as _r
    import pyarrow as _pa
    _f = _r.make_arrow_extension_type
    def _g(*a, **k):
        c = _f(*a, **k)
        one = c()
        ctypes.pythonapi.Py_IncRef(ctypes.py_object(one))
        c.__new__ = staticmethod(lambda cls, _c=c, _o=one: _o if cls is _c else _pa.ExtensionType.__new__(cls))
        c.__init__ = lambda self: None
        return c
    for _m in list(sys.modules.values()):
        if _m is not None and getattr(_m, "make_arrow_extension_type", None) is _f:
            _m.make_arrow_extension_type = _g
    """
)

MIN = textwrap.dedent(
    """
    import sys, pyarrow as pa, pyarrow.dataset as ds, pyarrow.parquet as pq, pyarrow.fs as fs
    class Ext(pa.ExtensionType):
        def __init__(self): super().__init__(pa.large_string(), "itl1422.ext")
        def __arrow_ext_serialize__(self): return b""
        @classmethod
        def __arrow_ext_deserialize__(cls, storage, data): return ONE if KEEP else cls()
    KEEP = sys.argv[3] == "keep"
    ONE = Ext()
    if KEEP:
        import ctypes
        ctypes.pythonapi.Py_IncRef(ctypes.py_object(ONE))
    pa.register_extension_type(ONE)
    path = sys.argv[1] + "/f.parquet"
    if sys.argv[2] == "save":
        values = pa.array(["a", "b"], pa.large_string())
        if sys.argv[3] != "plain": values = pa.ExtensionArray.from_storage(ONE, values)
        pq.write_table(pa.table({"x": values}), path)
    else:
        warm = globals().get("_warm")
        for _ in range(3):
            t = ds.dataset(path, format="parquet", filesystem=fs.LocalFileSystem()).to_table(
                use_threads=sys.argv[3] != "serial")
        assert t.num_rows == 2
        print("DONE", flush=True)
    """
)

INT_CHILD = textwrap.dedent(
    """
    import sys
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import DictSource
    @function_pod(output_keys="out")
    def produce(i: int) -> int:
        return i + 1
    if sys.argv[2] == "save":
        s = DictSource([{"k": 0, "i": 0}], tag_columns=["k"], tag_only_columns=True, source_id="one")
        job = PipelineJob(name="f", database=DeltaTableDatabase(sys.argv[1] + "/store"))
        with job: produce.pod(s, label="produce")
        job.run(); job.save(sys.argv[1] + "/job.json")
    else:
        job = PipelineJob.load(sys.argv[1] + "/job.json", database=DeltaTableDatabase(sys.argv[1] + "/store"))
    _, d = next(iter(job.nodes["produce"].as_source().iter_data()))
    assert d.as_dict() == {"out": 1}
    print("DONE", flush=True)
    """
)

SET_SAVE = textwrap.dedent(
    """
    import sys
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import DictSource
    @function_pod(output_keys="out")
    def produce(i: int) -> set[bytes]:
        return {b"y", b"x"}
    s = DictSource([{"k": 0, "i": 0}], tag_columns=["k"], tag_only_columns=True, source_id="one")
    job = PipelineJob(name="f", database=DeltaTableDatabase(sys.argv[1] + "/store"))
    with job: produce.pod(s, label="produce")
    job.run(); job.save(sys.argv[1] + "/job.json")
    """
)


def _module(name: str):
    path = ROOT / "tests" / "test_logical_types" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _py(code: str, *args: str, env: dict[str, str] | None = None, python: str = sys.executable):
    started = time.monotonic()
    with tempfile.TemporaryFile("w+") as out:
        proc = subprocess.Popen(
            [python, "-W", "ignore", "-c", code, *args], stdout=out, stderr=out,
            env={**os.environ, **(env or {})},
        )
        try:
            rc: object = proc.wait(DEADLINE)
        except subprocess.TimeoutExpired:
            proc.send_signal(signal.SIGKILL)
            proc.wait()
            rc = "HANG"
        out.seek(0)
        return rc, out.read(), time.monotonic() - started


def _prepare(work: Path) -> dict[str, tuple[str, list[str]]]:
    s = _module("test_set_of_primitives")
    n = _module("test_nested_logical_type_refusal")
    for d in ("set", "model", "int", "min", "minplain"):
        (work / d).mkdir()
    (work / "classes").mkdir()
    (work / "classes" / "classes.py").write_text(n._CLASSES)
    w = str(work)
    for code, args in (
        (SET_SAVE, [w + "/set"]),
        (n._SCRIPT, [w + "/classes", "save", w + "/model", "ModelFields"]),
        (INT_CHILD, [w + "/int", "save"]),
        (MIN, [w + "/min", "save", "ext"]),
        (MIN, [w + "/minplain", "save", "plain"]),
    ):
        rc, text, _ = _py(code, *args)
        if rc != 0:
            print(f"SAVE CHILD rc={rc}\n{text[-3000:]}")
    return {
        "set": (s._FRESH_READ, [w + "/set/job.json", w + "/set/store"]),
        "model": (n._SCRIPT, [w + "/classes", "load", w + "/model", "ModelFields"]),
        "int": (INT_CHILD, [w + "/int", "load"]),
        "min-ext": (MIN, [w + "/min", "load", "ext"]),
        "min-serial": (MIN, [w + "/min", "load", "serial"]),
        "min-keep": (MIN, [w + "/min", "load", "keep"]),
        "min-plain": (MIN, [w + "/minplain", "load", "plain"]),
    }


def test_stress(tmp_path: Path, capfd: pytest.CaptureFixture[str]) -> None:
    children = _prepare(tmp_path)
    env: dict[str, str] = {}
    if LINUX and shutil.which("gcc"):
        c = tmp_path / "t.c"
        c.write_text("#define _GNU_SOURCE\n#include <stdio.h>\n" + TRACER)
        subprocess.run(["gcc", "-shared", "-fPIC", "-o", str(tmp_path / "t.so"), str(c)], check=True)
        env = {"LD_PRELOAD": str(tmp_path / "t.so"), "PYTHONFAULTHANDLER": "0"}
        env.pop("PYTHONFAULTHANDLER")
    newest = ""
    if shutil.which("uv"):
        venv = tmp_path / "venv"
        subprocess.run(["uv", "venv", "-q", "--python", sys.executable, str(venv)], check=False)
        done = subprocess.run(
            ["uv", "pip", "install", "-q", "--python", str(venv / "bin/python"), "--upgrade", "pyarrow"],
            check=False, cwd=tmp_path,
        )
        if done.returncode == 0:
            newest = str(venv / "bin/python")
    matrix: list[tuple[str, str, str, str]] = []
    for amp, tag in (("", ""), (AMPLIFY, "+amp")):
        for child in ("set", "model"):
            matrix += [
                (child + tag, child, amp, sys.executable),
                (child + tag + "+serial", child, amp + SERIAL, sys.executable),
                (child + tag + "+single", child, amp + SINGLE, sys.executable),
            ]
        matrix.append(("int" + tag, "int", amp, sys.executable))
        for child in ("min-plain", "min-ext", "min-serial", "min-keep"):
            matrix.append((child + tag, child, amp, sys.executable))
        if newest:
            matrix.append(("min-ext-newest" + tag, "min-ext", amp, newest))
    picked = os.environ.get("ITL1422_ONLY")
    lines = [f"STRESS {platform.platform()} py{sys.version.split()[0]} cpus={os.cpu_count()}"]
    if newest:
        lines.append("newest " + _py("import pyarrow; print(pyarrow.__version__)", python=newest)[1].strip())
    stacks: dict[str, int] = collections.Counter()
    for label, child, pre, python in matrix:
        if picked and label not in picked.split(","):
            continue
        code, args = children[child]
        runs = RUNS if not label.startswith("min") else RUNS * 2
        with ThreadPoolExecutor(PARALLEL) as pool:
            got = list(pool.map(lambda _: _py(pre + code, *args, env=env, python=python), range(runs)))
        counts = collections.Counter(str(rc) for rc, _, _ in got)
        took = sorted(t for _, _, t in got)[len(got) // 2]
        lines.append(f"COUNT {label:26s} runs={runs} rc={dict(counts)} median={took:.2f}s")
        for rc, text, _ in got:
            if rc != 0:
                stacks[f"{label} rc={rc}\n" + text[-6000:]] += 1
        with capfd.disabled():
            print("\n" + lines[-1], flush=True)
    shown: dict[str, int] = collections.Counter()
    for text in stacks:
        label = text.split(" ", 1)[0]
        shown[label] += 1
        if shown[label] <= 3:
            lines.append("FAILURE " + text)
    with capfd.disabled():
        print("\n".join(lines), flush=True)
