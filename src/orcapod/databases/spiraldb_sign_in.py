"""Whether spiral-sdk can sign in without asking a person (ITL-1186).

spiral-sdk takes credentials from the environment, then from the file ``spiral login``
saved, and otherwise starts a device sign-in: it prints a code and waits for a person. A
source, a job or a notebook must not do that, so ``require_sign_in`` refuses first.

It decides from the environment and the file's presence alone: no client is made, no
request is sent and no credential is read. Whether a saved login still works (it expires
30 days after it was last renewed, and can be ended earlier) is not known here.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from pathlib import Path

from orcapod.errors import SpiralDBSignInError


def saved_login_path(environ: Mapping[str, str]) -> Path:
    """Return where ``spiral login`` saves credentials.

    Args:
        environ: The environment.
    """
    if given := environ.get("SPIRAL_AUTH_FILE"):
        return Path(given)
    home = Path(environ.get("HOME") or Path.home())
    if sys.platform == "darwin":
        folder = home / "Library" / "Application Support"
    else:
        folder = Path(environ.get("XDG_CONFIG_HOME") or home / ".config")
    name = "auth.dev.json" if environ.get("SPIRAL__DEV") else "auth.json"
    return folder / "pyspiral" / name


def require_sign_in() -> None:
    """Refuse when spiral-sdk would have to ask a person to sign in.

    Raises:
        SpiralDBSignInError: Saying what to run. No credential's value is read into it.
    """
    environ = os.environ
    if environ.get("SPIRAL_TOKEN") or environ.get("SPIRAL_WORKLOAD_ID"):
        return
    if environ.get("SPIRAL_CLIENT_ID") and (
        environ.get("SPIRAL_PRIVATE_KEY") or environ.get("SPIRAL_CLIENT_SECRET")
    ):
        return
    path = saved_login_path(environ)
    try:
        saved = path.stat().st_size > 0
    except OSError:
        saved = False
    if not saved:
        raise SpiralDBSignInError(
            f"no SpiralDB sign-in was found (no saved login in {path}, and none of "
            "SPIRAL_TOKEN, SPIRAL_WORKLOAD_ID, or SPIRAL_CLIENT_ID with "
            "SPIRAL_PRIVATE_KEY is set). Run 'uv run spiral login' in a terminal, then "
            "try again."
        )
