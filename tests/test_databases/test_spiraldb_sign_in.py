"""``require_sign_in``: refuse before spiral-sdk would ask a person to sign in (ITL-1186).

No test makes a client or reads a credential: the rule looks at the environment and at
whether the saved login's file has content.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from orcapod.databases import SpiralDBConnector, spiraldb_connector
from orcapod.databases.spiraldb_sign_in import require_sign_in, saved_login_path
from orcapod.errors import SpiralDBSignInError
from orcapod.sources import SpiralDBTableSource

_VARIABLES = (
    "SPIRAL_TOKEN",
    "SPIRAL_WORKLOAD_ID",
    "SPIRAL_CLIENT_ID",
    "SPIRAL_PRIVATE_KEY",
    "SPIRAL_CLIENT_SECRET",
    "SPIRAL_AUTH_FILE",
    "SPIRAL__DEV",
    "XDG_CONFIG_HOME",
)


@pytest.fixture(autouse=True)
def empty_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An environment with no credential and an empty home."""
    for name in _VARIABLES:
        monkeypatch.delenv(name, raising=False)
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    return home


def _save(path: Path, text: str = "{}") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def test_an_empty_home_is_refused_naming_the_command() -> None:
    with pytest.raises(SpiralDBSignInError, match="uv run spiral login"):
        require_sign_in()


@pytest.mark.parametrize(
    "given",
    [
        {"SPIRAL_TOKEN": "t"},
        {"SPIRAL_WORKLOAD_ID": "w"},
        {"SPIRAL_CLIENT_ID": "c", "SPIRAL_PRIVATE_KEY": "k"},
        {"SPIRAL_CLIENT_ID": "c", "SPIRAL_CLIENT_SECRET": "s"},
    ],
)
def test_each_credential_in_the_environment_is_accepted(
    given: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in given.items():
        monkeypatch.setenv(name, value)
    require_sign_in()


@pytest.mark.parametrize(
    "given",
    [
        {"SPIRAL_TOKEN": ""},
        {"SPIRAL_CLIENT_ID": "c"},
        {"SPIRAL_PRIVATE_KEY": "k"},
        {"SPIRAL_CLIENT_ID": "c", "SPIRAL_PRIVATE_KEY": ""},
    ],
)
def test_half_a_credential_is_refused(
    given: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in given.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(SpiralDBSignInError) as refused:
        require_sign_in()
    for value in given.values():
        assert not value or f"={value}" not in str(refused.value)


@pytest.mark.parametrize("platform", ["darwin", "linux"])
def test_the_saved_login_of_each_platform(
    platform: str, empty_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "platform", platform)
    folder = {
        "darwin": empty_home / "Library" / "Application Support",
        "linux": empty_home / ".config",
    }[platform]
    path = saved_login_path({"HOME": str(empty_home)})
    assert path == folder / "pyspiral" / "auth.json"
    _save(path, "")
    with pytest.raises(SpiralDBSignInError):  # an empty file is no login
        require_sign_in()
    _save(path)
    require_sign_in()


def test_xdg_moves_the_file_on_linux_only(
    tmp_path: Path, empty_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    environ = {"HOME": str(empty_home), "XDG_CONFIG_HOME": str(tmp_path / "xdg")}
    monkeypatch.setattr(sys, "platform", "linux")
    assert saved_login_path(environ) == tmp_path / "xdg" / "pyspiral" / "auth.json"
    monkeypatch.setattr(sys, "platform", "darwin")
    assert str(empty_home) in str(saved_login_path(environ))


def test_the_override_names_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "elsewhere" / "login.json"
    monkeypatch.setenv("SPIRAL_AUTH_FILE", str(path))
    with pytest.raises(SpiralDBSignInError, match="login.json"):
        require_sign_in()
    _save(path)
    require_sign_in()


def test_a_source_and_a_connector_refuse_before_a_client_is_made(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NoClient:
        """A module whose client must not be made."""

        @staticmethod
        def Client(**_options: object) -> None:
            raise AssertionError("a client was made")

    monkeypatch.setattr(spiraldb_connector, "sp", NoClient)
    with pytest.raises(SpiralDBSignInError):
        SpiralDBConnector("p")
    with pytest.raises(SpiralDBSignInError):
        SpiralDBTableSource("p", "t")


def test_pyspiral_in_place_of_the_sdk_is_named(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Pyspiral:
        """A ``spiral`` module without ``Client``."""

    monkeypatch.setattr(spiraldb_connector, "sp", Pyspiral)
    with pytest.raises(ImportError, match="pyspiral"):
        SpiralDBConnector("p")
