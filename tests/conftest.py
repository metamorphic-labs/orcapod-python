"""Root conftest.py for the test suite.

Registers the ``--postgres`` CLI flag and skips ``@pytest.mark.postgres``
tests by default.  Pass ``--postgres`` to opt in::

    uv run pytest --postgres

In CI (``CI`` set to ``true``, as GitHub Actions does) the tests run in parallel on every
core of the runner (pytest-xdist, ``-n auto``) unless ``-n`` is given (ITL-1181). This is
the one place that turns it on without ``-n``: it is how the organization's required pytest
check, whose command this repository cannot change, runs in parallel. Local runs stay
serial; pass ``-n 4`` to run in parallel on a machine with cores and memory to spare.
"""

from __future__ import annotations

import os

import pytest


def _running_in_ci() -> bool:
    """Whether the tests run in CI: ``CI`` is ``true`` (or ``1``), as GitHub Actions sets it."""
    return os.environ.get("CI", "").strip().lower() in {"true", "1"}


@pytest.hookimpl(tryfirst=True)
def pytest_cmdline_main(config: pytest.Config) -> None:
    """In CI, run the tests on every core unless ``-n`` was given (ITL-1181).

    Sets ``-n auto`` before pytest-xdist reads the option: both hooks are ``tryfirst``,
    and pytest calls this conftest's before the plugin's, which was registered earlier.
    ``-n 0`` (or ``-p no:xdist``) keeps a CI run serial. A worker is left alone: xdist
    clears ``-n`` in each worker before calling this hook there, and setting it again
    would make every worker start workers of its own.
    """
    if not _running_in_ci() or not config.pluginmanager.has_plugin("xdist"):
        return
    if hasattr(config, "workerinput"):
        return
    if config.getoption("numprocesses") is None:
        config.option.numprocesses = "auto"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--postgres",
        action="store_true",
        default=False,
        help="Run tests that require a live PostgreSQL instance.",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Skip @pytest.mark.postgres tests unless --postgres is given."""
    if config.getoption("--postgres"):
        return

    skip_marker = pytest.mark.skip(
        reason="PostgreSQL tests are opt-in; pass --postgres to run them"
    )
    for item in items:
        if any(mark.name == "postgres" for mark in item.iter_markers()):
            item.add_marker(skip_marker)
