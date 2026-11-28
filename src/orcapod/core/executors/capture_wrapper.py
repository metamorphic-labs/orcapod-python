"""Inline capture wrapper for remote execution environments.

The wrapper is returned as a closure so that cloudpickle serializes it by
bytecode rather than by module reference.  This means remote workers (e.g.
Ray) do **not** need ``orcapod`` installed — only the standard library is
required on the worker side.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from orcapod.utils.output_containers import output_container_mismatch


CaptureResponse = tuple[Any, str, str, str] | tuple[None, str, str, str, str]


def make_capture_wrapper(name: str | None = None) -> Callable[..., Any]:
    """Return a capture wrapper suitable for remote execution.

    Args:
        name: If provided, the wrapper's ``__name__`` and ``__qualname__``
            are set to this value so that remote frameworks (e.g. Ray) report
            the original function name in dashboards and metrics rather than
            the generic ``_capture``.

    On success the wrapper returns a 4-tuple
    ``(raw_result, stdout_log, stderr_log, python_logs)``.
    If a declared return container does not match, it returns a 5-tuple
    ``(None, stdout_log, stderr_log, python_logs, returned_type)`` so the driver
    can report the mismatch without serializing the invalid value.

    On failure the wrapper **raises** a ``_CapturedTaskError`` that carries
    the captured I/O alongside the original exception.  This lets the remote
    framework's normal error handling proceed (retries, error wrapping, etc.)
    while still delivering captured output to the driver.
    """

    check_container = output_container_mismatch

    def _capture(
        fn: Any, kwargs: dict[str, Any], return_container: type | None = None
    ) -> CaptureResponse:
        import asyncio
        import inspect
        import io
        import logging
        import os
        import sys
        import tempfile
        import traceback as _tb

        # -- _CapturedTaskError defined inline so cloudpickle serializes
        #    it by bytecode — no orcapod import needed on the worker. --

        class _CapturedTaskError(Exception):
            """Wraps a user exception with captured I/O from the worker."""

            def __init__(self, cause, stdout_log, stderr_log, python_logs, tb):
                super().__init__(str(cause))
                self.cause = cause
                self.captured_stdout_log = stdout_log
                self.captured_stderr_log = stderr_log
                self.captured_python_logs = python_logs
                self.captured_traceback = tb

            def __reduce__(self):
                return (
                    self.__class__,
                    (
                        self.cause,
                        self.captured_stdout_log,
                        self.captured_stderr_log,
                        self.captured_python_logs,
                        self.captured_traceback,
                    ),
                )

        stdout_tmp = tempfile.TemporaryFile()
        stderr_tmp = tempfile.TemporaryFile()
        orig_stdout_fd = os.dup(1)
        orig_stderr_fd = os.dup(2)
        orig_sys_stdout = sys.stdout
        orig_sys_stderr = sys.stderr
        sys_stdout_buf = io.StringIO()
        sys_stderr_buf = io.StringIO()
        log_records: list = []

        fmt = logging.Formatter("%(levelname)s:%(name)s:%(message)s")

        class _H(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                log_records.append(fmt.format(record))

        handler = _H()
        root_logger = logging.getLogger()
        orig_level = root_logger.level
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

        raw_result = None
        returned_type = None
        exc_info: tuple | None = None
        try:
            sys.stdout.flush()
            sys.stderr.flush()
            os.dup2(stdout_tmp.fileno(), 1)
            os.dup2(stderr_tmp.fileno(), 2)
            sys.stdout = sys_stdout_buf
            sys.stderr = sys_stderr_buf
            try:
                result = fn(**kwargs)
                # When checking a declared container, await only coroutine
                # functions, as the local executor does. A coroutine returned
                # by a synchronous function is itself the value to validate.
                if inspect.iscoroutine(result) and (
                    return_container is None or inspect.iscoroutinefunction(fn)
                ):
                    raw_result = asyncio.run(result)
                else:
                    raw_result = result
                if return_container is not None:
                    returned_type = check_container(raw_result, return_container)
            except Exception as e:
                exc_info = (e, _tb.format_exc())
        finally:
            sys.stdout = orig_sys_stdout
            sys.stderr = orig_sys_stderr
            os.dup2(orig_stdout_fd, 1)
            os.dup2(orig_stderr_fd, 2)
            os.close(orig_stdout_fd)
            os.close(orig_stderr_fd)
            root_logger.removeHandler(handler)
            root_logger.setLevel(orig_level)
            stdout_tmp.seek(0)
            stderr_tmp.seek(0)
            cap_stdout = (
                stdout_tmp.read().decode("utf-8", errors="replace")
                + sys_stdout_buf.getvalue()
            )
            cap_stderr = (
                stderr_tmp.read().decode("utf-8", errors="replace")
                + sys_stderr_buf.getvalue()
            )
            stdout_tmp.close()
            stderr_tmp.close()

        python_logs = "\n".join(log_records)

        if exc_info is not None:
            raise _CapturedTaskError(
                cause=exc_info[0],
                stdout_log=cap_stdout,
                stderr_log=cap_stderr,
                python_logs=python_logs,
                tb=exc_info[1],
            ) from exc_info[0]

        if returned_type is not None:
            return None, cap_stdout, cap_stderr, python_logs, returned_type
        return raw_result, cap_stdout, cap_stderr, python_logs

    if name is not None:
        _capture.__name__ = name
        _capture.__qualname__ = name

    return _capture
