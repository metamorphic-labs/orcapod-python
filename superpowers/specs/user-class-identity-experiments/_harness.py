"""Shared helpers for the ITL-881 user-class identity experiments.

Every experiment drives Orcapod through its public interface (``DictSource``,
``@function_pod``, ``PipelineJob``, ``DeltaTableDatabase``, ``as_table``,
``content_hash``, ``pipeline_hash``). Scenarios that depend on where a class is
defined run in fresh subprocesses, so each one starts with an empty type registry
and its own ``__main__``.

Run any experiment from the repository root with ``uv run python <script>``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

EXPERIMENT_DIR = Path(__file__).resolve().parent
JSON_MARKER = "JSON:"


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------


def write_files(root: Path, files: Mapping[str, str]) -> None:
    """Write dedented source files under ``root``, creating parent directories.

    Args:
        root: Directory to write into.
        files: Mapping of relative path to file content.
    """
    for rel, content in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        _ = path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")


def run_python(
    argv: Sequence[str],
    *,
    cwd: Path,
    extra_paths: Iterable[Path] = (),
    env: Mapping[str, str] | None = None,
    timeout: float = 180.0,
) -> dict[str, Any]:
    """Run ``python <argv>`` in a fresh process and return what it reported.

    The child reports by printing one line ``JSON:<payload>`` (see ``report``).
    ``PYTHONPATH`` gets ``cwd``, the experiment directory (so children can import
    this module) and ``extra_paths``.

    Args:
        argv: Arguments after the interpreter, e.g. ``["-c", code]`` or
            ``["-m", "pkg.run"]``.
        cwd: Working directory of the child.
        extra_paths: Further directories to put on ``PYTHONPATH``.
        env: Extra environment variables.
        timeout: Seconds before the child is killed. A child that reported and
            then hung is returned with its payload and ``"error": "TIMEOUT ..."``.

    Returns:
        ``{"ok": bool, "result": payload | None, "error": last stderr line}``.
    """
    paths = [str(cwd), str(EXPERIMENT_DIR), *(str(p) for p in extra_paths)]
    child_env = dict(os.environ)
    child_env["PYTHONPATH"] = os.pathsep.join(paths)
    child_env.update(env or {})
    try:
        proc = subprocess.run(
            [sys.executable, *argv],
            cwd=cwd,
            env=child_env,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        out = exc.stdout.decode() if isinstance(exc.stdout, bytes) else exc.stdout or ""
        payload_t: Any = None
        for line in out.splitlines():
            if line.startswith(JSON_MARKER):
                payload_t = json.loads(line[len(JSON_MARKER) :])
        return {
            "ok": False,
            "result": payload_t,
            "error": f"TIMEOUT after {timeout:.0f}s (process did not exit)",
        }
    payload: Any = None
    for line in proc.stdout.splitlines():
        if line.startswith(JSON_MARKER):
            payload = json.loads(line[len(JSON_MARKER) :])
    error = ""
    if proc.returncode != 0:
        lines = [ln for ln in proc.stderr.strip().splitlines() if ln.strip()]
        error = lines[-1] if lines else f"exit code {proc.returncode}"
    return {
        "ok": proc.returncode == 0,
        "result": payload,
        "error": error,
        "log": log_messages(proc.stderr),
    }


def log_messages(stderr: str) -> list[str]:
    """Return the distinct log messages a child printed, without tracebacks.

    Python's last-resort logging handler prints a WARNING or ERROR record as its bare
    message, which is what a notebook user sees in red. Rust ``deltalake`` lines,
    traceback frames and indented continuation lines are dropped.

    Args:
        stderr: The child's standard error.

    Returns:
        Distinct messages in first-seen order, each cut to 160 characters.
    """
    seen: dict[str, None] = {}
    for line in stderr.splitlines():
        if not line.strip() or line[0].isspace() or line.startswith(("[", "Traceback")):
            continue
        seen.setdefault(line[:160], None)
    return list(seen)


def run_code(
    code: str, *, cwd: Path, extra_paths: Iterable[Path] = ()
) -> dict[str, Any]:
    """Run a dedented code string as ``__main__`` in a fresh process.

    Args:
        code: Python source. Classes it defines have ``__module__ == "__main__"``.
        cwd: Working directory of the child.
        extra_paths: Further directories to put on ``PYTHONPATH``.

    Returns:
        See ``run_python``.
    """
    return run_python(["-c", textwrap.dedent(code)], cwd=cwd, extra_paths=extra_paths)


def report(payload: Any) -> None:
    """Print ``payload`` as the single JSON line a parent reads back, then exit.

    The child exits with ``os._exit(0)`` straight after flushing. A process that has
    read Delta tables sometimes hangs at interpreter shutdown inside Arrow's thread-pool
    destructor (seen while writing these experiments; not isolated, out of scope for
    ITL-881), and skipping the shutdown keeps every experiment from stalling on it.

    Args:
        payload: Any JSON-serialisable value.
    """
    print(JSON_MARKER + json.dumps(payload, default=str))
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


def capture(fn: Callable[[], Any]) -> Any:
    """Call ``fn`` and return its value, or ``"ERROR <Type>: <message>"``.

    Args:
        fn: Zero-argument callable.

    Returns:
        The return value, or an error string naming the exception.
    """
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - experiments record every failure
        message = str(exc).splitlines()[0] if str(exc) else ""
        return f"ERROR {type(exc).__name__}: {message}"


# ---------------------------------------------------------------------------
# Identity report (runs inside a child process)
# ---------------------------------------------------------------------------


def delta_table_paths(root: Path) -> list[str]:
    """List the Delta tables under ``root`` as paths relative to it.

    Args:
        root: Directory given to ``DeltaTableDatabase``.

    Returns:
        Sorted relative paths of every directory holding a ``_delta_log``.
    """
    return sorted(
        str(p.parent.relative_to(root)) for p in root.rglob("_delta_log") if p.is_dir()
    )


def extension_info(arrow_type: Any) -> dict[str, Any]:
    """Describe an Arrow type as stored: extension name, metadata and storage type.

    Args:
        arrow_type: A ``pa.DataType``.

    Returns:
        ``{"extension_name", "extension_metadata", "storage_type"}``; the first two
        are ``None`` for a non-extension type.
    """
    import pyarrow as pa

    if isinstance(arrow_type, pa.ExtensionType):
        raw = arrow_type.__arrow_ext_serialize__()
        return {
            "extension_name": arrow_type.extension_name,
            "extension_metadata": raw.decode("utf-8") if raw else "",
            "storage_type": str(arrow_type.storage_type),
        }
    return {
        "extension_name": None,
        "extension_metadata": None,
        "storage_type": str(arrow_type),
    }


def identity_report(value: Any, cls: type, db_root: Path) -> dict[str, Any]:
    """Collect every place a user class's identity can show up.

    Builds a one-row ``DictSource`` with data column ``p`` holding ``value``, a
    function pod ``shift(p: cls) -> cls`` defined in ``__main__`` of the caller,
    and runs it in a ``PipelineJob`` over a ``DeltaTableDatabase`` at ``db_root``.

    Args:
        value: An instance of ``cls``.
        cls: The user class under test.
        db_root: Empty directory for the Delta database.

    Returns:
        A flat mapping of component name to observed value.
    """
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import DictSource

    out: dict[str, Any] = {}
    source = DictSource(
        data=[{"k": "r1", "p": value}], tag_columns=["k"], source_id="s"
    )
    _, data_schema = source.output_schema()
    out["python schema (repr)"] = repr(data_schema)
    table = source.as_table(all_info=True)
    info = extension_info(table.schema.field("p").type)
    out["arrow extension name"] = info["extension_name"]
    out["arrow extension metadata"] = info["extension_metadata"]
    out["arrow storage type"] = info["storage_type"]
    out["source content_hash"] = source.content_hash().to_string()
    out["source pipeline_hash"] = source.pipeline_hash().to_string()
    out["system tag columns"] = sorted(
        n for n in table.column_names if n.startswith("_tag")
    )
    _, data = next(iter(source.iter_data()))
    out["row data content_hash"] = data.content_hash().to_string()

    def shift(p: Any) -> Any:
        return p

    shift.__annotations__ = {"p": cls, "return": cls}
    pod = function_pod(output_keys="q")(shift)
    out["pod content_hash"] = pod.pod.content_hash().to_string()
    out["pod pipeline_hash"] = pod.pod.pipeline_hash().to_string()

    db = DeltaTableDatabase(db_root)
    job = PipelineJob(name="j", store=db)
    with job:
        src = DictSource(
            data=[{"k": "r1", "p": value}], tag_columns=["k"], source_id="s"
        )
        pod.pod(src, label="shift")
    _ = job.run()
    node: Any = job.nodes["shift"]
    out["node pipeline_hash"] = node.pipeline_hash().to_string()
    out["node identity path"] = list(node.node_identity_path)
    out["DB table paths"] = delta_table_paths(db_root)
    out["read back q"] = repr(node.as_table().column("q").to_pylist())
    return out


def print_table(
    rows: Sequence[str], columns: Mapping[str, Mapping[str, Any]], baseline: str
) -> None:
    """Print a component-by-variant table, marking values equal to the baseline.

    Args:
        rows: Component names, in display order.
        columns: Variant name to ``{component: value}``.
        baseline: Variant every other column is compared against.
    """
    base = columns[baseline]
    for row in rows:
        print(f"\n## {row}")
        for name, values in columns.items():
            value = values.get(row, "<missing>")
            if name != baseline and value == base.get(row):
                shown = "== baseline"
            else:
                shown = json.dumps(value, default=str)
            print(f"  {name:<34} {shown}")
