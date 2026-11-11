"""Pytest plugin: run the suite with option (c) applied, without editing ``src/``.

Usage (repository root):
    PYTHONPATH=superpowers/specs/itl-892-exit-hang \
        uv run pytest -p native_fs_pytest_plugin tests/
"""

from repro_orcapod import apply_patch

apply_patch("native-fs")
