"""Checks for public names declared by Orcapod packages."""

from importlib import import_module
from pkgutil import walk_packages
from typing import cast

import orcapod


def test_package_exports_resolve_to_objects() -> None:
    """Every declared export in the root package and subpackages resolves."""
    packages = [orcapod.__name__]
    packages.extend(
        entry.name
        for entry in walk_packages(orcapod.__path__, orcapod.__name__ + ".")
        if entry.ispkg
    )

    unresolved: list[str] = []
    for package in packages:
        module = import_module(package)
        exports = cast("tuple[str, ...] | list[str]", getattr(module, "__all__", ()))
        for name in exports:
            if getattr(module, name, None) is None:
                unresolved.append(f"{package}.{name}")

    assert not unresolved, "Unresolved package exports:\n" + "\n".join(
        sorted(unresolved)
    )
