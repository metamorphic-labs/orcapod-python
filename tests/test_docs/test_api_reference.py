"""Keep the public export lists and generated API reference in sync."""

from __future__ import annotations

import re
from collections.abc import Sequence
from importlib import import_module
from pathlib import Path
from typing import cast

import griffe
from griffe import Alias, Module, Object

REPO_ROOT = Path(__file__).resolve().parents[2]
PUBLIC_MODULES = (
    "orcapod",
    "orcapod.databases",
    "orcapod.errors",
    "orcapod.nodes",
    "orcapod.operators",
    "orcapod.pipeline",
    "orcapod.sources",
    "orcapod.streams",
    "orcapod.types",
)

# These namespaces are outside the public reference. The reasons also apply
# to their nested modules.
EXCLUDED_NAMESPACES = {
    "orcapod._version": "Generated build metadata.",
    "orcapod.contexts": "Public API documented in concept pages. A dedicated reference page will follow separately.",
    "orcapod.core": "Implementation modules behind the public re-exports.",
    "orcapod.extensions": "Implementation namespace for the top-level extension API.",
    "orcapod.hashing": "Hashing internals and dedicated hashing documentation.",
    "orcapod.logical_types": "Logical type internals and concept documentation.",
    "orcapod.pipeline.dag": "Internal graph interfaces and implementations.",
    "orcapod.pipeline.diagram": "Implementation module for the public PipelineDiagram.",
    "orcapod.pipeline.networkx_backend": "Internal graph backend.",
    "orcapod.protocols": "Internal interfaces between Orcapod components.",
    "orcapod.semantic_types": "Internal semantic type conversion and inference.",
    "orcapod.structural": "Implementation namespace for the top-level dataclass decorator.",
}

EXCLUDED_EXPORTS = {
    "orcapod.__version__": "Version metadata supplied by the package build.",
    "orcapod.Path": "Stable alias of pathlib.Path, documented by Python.",
    "orcapod.UPath": "Stable alias of upath.UPath, documented by universal-pathlib.",
    "orcapod.UUID": "Stable alias of uuid.UUID, documented by Python.",
    "orcapod.databases": "Package link in the API overview.",
    "orcapod.nodes": "Package link in the API overview.",
    "orcapod.operators": "Package link in the API overview.",
    "orcapod.sources": "Package link in the API overview.",
    "orcapod.streams": "Package link in the API overview.",
    "orcapod.types": "Package link in the API overview.",
    "orcapod.nodes.FunctionNodeBase": "Internal base shared by function node implementations.",
    "orcapod.nodes.GraphNode": "Internal base for nodes in a pipeline graph.",
    "orcapod.nodes.JobNode": "Internal base for nodes bound to a job.",
    "orcapod.nodes.OperatorNodeBase": "Internal base shared by operator node implementations.",
    "orcapod.nodes.SourceNodeBase": "Internal base shared by source node implementations.",
    "orcapod.operators.PROVISIONAL_OPERATORS": "Bookkeeping for provisional operator hashes.",
    "orcapod.pipeline.ExecutionContext": "Placeholder for pipeline execution configuration.",
    "orcapod.pipeline.GraphProtocol": "Internal graph backend interface.",
    "orcapod.pipeline.OrcaDAG": "Internal representation of a pipeline graph.",
    "orcapod.pipeline.PIPELINE_FORMAT_VERSION": "Serialization metadata for pipeline blueprints.",
    "orcapod.pipeline.PIPELINE_JOB_FORMAT_VERSION": "Serialization metadata for pipeline jobs.",
    "orcapod.sources.GLOBAL_SOURCE_REGISTRY": "Shared process-wide source registry instance.",
    "orcapod.sources.PROVISIONAL_SOURCES": "Bookkeeping for provisional source hashes.",
}


def test_public_exports_have_api_reference_entries() -> None:
    package = griffe.load(
        "orcapod",
        search_paths=[str(REPO_ROOT / "src")],
        resolve_aliases=True,
        resolve_external=False,
    )
    modules = [package]
    unclassified: list[str] = []
    for source_module in modules:
        modules.extend(
            child
            for child in source_module.members.values()
            if isinstance(child, Module)
        )
        if source_module.exports and source_module.path not in PUBLIC_MODULES:
            if not any(
                source_module.path == namespace
                or source_module.path.startswith(namespace + ".")
                for namespace in EXCLUDED_NAMESPACES
            ):
                unclassified.append(source_module.path)

    def reference_key(path: str) -> tuple[str, str]:
        # Preserve the exported spelling while counting existing entries for
        # the same object through an alias (e.g. orcapod.core.operators.Join).
        obj = cast("Object | Alias", package[path.removeprefix("orcapod.")])
        target = obj.final_target if isinstance(obj, Alias) else obj
        return path.rsplit(".", 1)[-1], target.path

    entries = {
        reference_key(directive.group(1))
        for page in (REPO_ROOT / "docs" / "api").glob("*.md")
        for directive in re.finditer(
            r"^::: +([\w.]+) *$", page.read_text(encoding="utf-8"), re.M
        )
    }
    exports: set[str] = set()
    for module_name in PUBLIC_MODULES:
        module = import_module(module_name)
        members = cast("dict[str, object]", vars(module))
        if module_name != "orcapod.errors":
            assert "__all__" in members, (
                f"{module_name} must declare its public exports"
            )
        names = cast("Sequence[str]", members.get("__all__", ()))
        exports.update(f"{module_name}.{name}" for name in names)
        if module_name == "orcapod.errors":
            # The errors module has no __all__. Its own exception and warning
            # classes are the public entries for this page.
            exports.update(
                f"{module_name}.{name}"
                for name, value in members.items()
                if isinstance(value, type)
                and value.__module__ == module_name
                and issubclass(value, Exception)
                and not name.startswith("_")
            )

    assert all(EXCLUDED_NAMESPACES.values()) and all(EXCLUDED_EXPORTS.values())
    assert not unclassified, f"Classify namespaces with __all__: {sorted(unclassified)}"
    assert not (stale := EXCLUDED_EXPORTS.keys() - exports), (
        f"Remove stale API exclusions: {sorted(stale)}"
    )
    missing = sorted(
        path
        for path in exports - EXCLUDED_EXPORTS.keys()
        if reference_key(path) not in entries
    )
    assert not missing, (
        "Add API reference entries or reasoned exclusions for:\n" + "\n".join(missing)
    )
