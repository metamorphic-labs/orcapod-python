"""Data function type declarations: what a type means and what its result table stores.

A function result table lives at

    <function>/<output schema hash>/v<function major>/<type>.v<major>/rev<revision>/rdb_v<N>

and each component has one owner (developer, November 18, 2026, ITL-1259):

* ``<type>.v<major>`` (``python.function.v1``) is the data function type and its
  major: what a function of the type means and how it is run. It is the last part of
  every function's uri, so it enters the identity: a new major moves hashes.
* ``rev<revision>`` (``rev1``) is the revision of the type's own record: the columns the
  type stores with every result row about the function and its execution
  (``pf_var_*``, ``pf_exec_*``). It is in the result table path only, never in a uri,
  hash, source-info token or record id: a new revision is a new result table for the
  type and moves no hash.
* ``rdb_v<N>`` is the layout of the columns every result table has, whatever its type
  (``ResultCache``, ``COMMON_RESULT_COLUMNS``). Path only.

The test between a new major and the other two: is a row stored before the change still
a correct result for the same input? No: a new major. Yes: a new revision (the type's
columns) or a new ``rdb`` version (the common columns).

A declaration is the source of truth for its type: the data function builds its stored
columns from it, the result cache takes the revision from it, and each released
declaration is also written out as a pinned snapshot (``data_function_types/``,
``scripts/data_function_types.py``). This module holds the declaration classes only and
imports nothing of Orcapod; each type's declaration is beside its class (the Python
function type's, ``PYTHON_FUNCTION``, in ``core/data_function.py``).
"""

from __future__ import annotations

import json
import typing
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StoredColumn:
    """One column a result table stores with every row.

    Attributes:
        name: The column name without its group prefix (``pf_var_`` and the like).
        python_type: The Python type of the value, as the data function reports it.
        arrow_type: The Arrow type of the stored column, as ``str(pa.DataType)`` spells
            it (``large_string``).
        nullable: Whether the stored column is nullable.
        meaning: One line on what the value is.
        hash_label: For a hash column, the label of the algorithm that produced it
            (a label constant, never a literal); ``None`` otherwise.
        covers: For a hash column, exactly what the hashed input holds.
    """

    name: str
    python_type: Any
    arrow_type: str
    nullable: bool
    meaning: str
    hash_label: str | None = None
    covers: str | None = None

    def to_snapshot(self) -> dict[str, Any]:
        """The column as JSON-ready data, for the declaration snapshot."""
        entry: dict[str, Any] = {
            "name": self.name,
            "python_type": _type_text(self.python_type),
            "arrow_type": self.arrow_type,
            "nullable": self.nullable,
            "meaning": self.meaning,
        }
        if self.hash_label is not None:
            entry["hash_label"] = self.hash_label
            entry["covers"] = self.covers
        return entry


def _type_text(python_type: Any) -> str:
    """A Python type as text: ``str`` for a class, the generic alias as written."""
    if isinstance(python_type, type) and not typing.get_args(python_type):
        return python_type.__name__
    return str(python_type)


@dataclass(frozen=True)
class DataFunctionTypeDeclaration:
    """The declaration of one data function type at one major and revision.

    Attributes:
        type_name: The type's name (``python.function``).
        major: The type's major: what a function of the type means and how it is run.
        revision: The revision of the type's record (the columns below).
        identity: The parts of a function's uri, in order, one line each.
        run_rules: How a function of the type is run, one rule per line: what the
            major stands for.
        variation: The columns describing the function (``pf_var_`` prefix).
        execution: The columns describing how a row was computed (``pf_exec_``).
    """

    type_name: str
    major: int
    revision: int
    identity: tuple[str, ...]
    run_rules: tuple[str, ...]
    variation: tuple[StoredColumn, ...]
    execution: tuple[StoredColumn, ...]

    @property
    def type_id(self) -> str:
        """The type and its major, the last part of every function's uri."""
        return f"{self.type_name}.v{self.major}"

    @property
    def revision_component(self) -> str:
        """The result table path component of the revision (``rev1``)."""
        return f"rev{self.revision}"

    def to_snapshot(self) -> dict[str, Any]:
        """The declaration as JSON-ready data, the content of its snapshot file."""
        return {
            "type_name": self.type_name,
            "major": self.major,
            "revision": self.revision,
            "type_id": self.type_id,
            "revision_component": self.revision_component,
            "identity": list(self.identity),
            "run_rules": list(self.run_rules),
            "variation": [column.to_snapshot() for column in self.variation],
            "execution": [column.to_snapshot() for column in self.execution],
        }


def snapshot_path(declaration: DataFunctionTypeDeclaration) -> str:
    """Where a declaration's snapshot is, relative to ``data_function_types/``.

    ``<type name>/v<major>-rev<revision>.json``: ``python.function/v1-rev1.json``.
    """
    return (
        f"{declaration.type_name}/v{declaration.major}-rev{declaration.revision}.json"
    )


def snapshot_text(declaration: DataFunctionTypeDeclaration) -> str:
    """The snapshot file's text: canonical JSON (sorted keys, two-space indent)."""
    return json.dumps(declaration.to_snapshot(), indent=2, sort_keys=True) + "\n"
