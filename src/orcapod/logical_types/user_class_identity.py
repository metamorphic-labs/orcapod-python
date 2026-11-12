"""Identity of user classes (dataclasses and pydantic models).

A user class is identified by an *identity record* with three components (ITL-881,
spec ``superpowers/specs/2026-11-10-user-class-identity-v0.3.md``):

- ``kind``: ``"dataclass"`` or ``"pydantic"``.
- ``name``: a declared name; otherwise ``__qualname__`` for a class defined in
  ``__main__`` (a notebook or script) and ``module.qualname`` for a module class.
- ``fields``: the stored fields as ``[name, type token]`` pairs in declared order.
  Order is part of the identity: a dataclass compares its fields as an ordered tuple,
  and a pydantic model dumps and iterates in declared order, so two classes with the
  same fields in another order are different types.

The *identity string* ``f"{name}@{fingerprint}"`` is the class's logical type name and
Arrow extension name. ``identity_string`` is the only place that defines its format.

This module also holds the process-wide state that belongs to classes rather than to a
data context: declared names and the table of classes seen under each
``(kind, name)``, which the read path uses to find a class for a stored column.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
import sys
import typing
import unicodedata
import warnings
import weakref
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, Literal

from orcapod.errors import RecursiveTypeError
from orcapod.utils.canonical_type_names import canonical_type_name

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol

KIND_DATACLASS = "dataclass"
KIND_PYDANTIC = "pydantic"

#: Modules whose classes are "defined in a notebook or script". ``__mp_main__`` is the
#: name a ``multiprocessing`` spawn child gives the main module.
MAIN_MODULES: frozenset[str] = frozenset({"__main__", "__mp_main__"})

#: Prefix reserved for Orcapod's own types (``orcapod.path``, ``orcapod.uuid``, ...).
RESERVED_PREFIX = "orcapod."

#: Number of hex digits of the SHA-256 fingerprint in an identity string (the full digest).
FINGERPRINT_HEX_DIGITS = 64

_NAME_FORBIDDEN = re.compile(r"[@\s]")

# Declared names, keyed by class object (weak, so classes from re-run cells can go away).
_declared: weakref.WeakKeyDictionary[type, str] = weakref.WeakKeyDictionary()
# The name each class object was built under; a class has one name per process.
_built: weakref.WeakKeyDictionary[type, str] = weakref.WeakKeyDictionary()
# The identity string and record each class object was built with.
_identity_of: weakref.WeakKeyDictionary[type, str] = weakref.WeakKeyDictionary()
_record_of: weakref.WeakKeyDictionary[type, dict[str, Any]] = (
    weakref.WeakKeyDictionary()
)
# Classes seen under each (kind, name), most recent last.
_known: dict[tuple[str, str], list[weakref.ref[type]]] = {}
# Names that already produced the one-time ``__main__`` notice.
_noticed_main_names: set[str] = set()


# ---------------------------------------------------------------------------
# Kinds and names
# ---------------------------------------------------------------------------


def class_kind(cls: Any) -> str | None:
    """Return the kind of a user class, or ``None`` if it is not one.

    Args:
        cls: Any object.

    Returns:
        ``"dataclass"`` for a dataclass type (including pydantic dataclasses),
        ``"pydantic"`` for a ``pydantic.BaseModel`` subclass, else ``None``.
    """
    if not isinstance(cls, type):
        return None
    if dataclasses.is_dataclass(cls):
        return KIND_DATACLASS
    from pydantic import BaseModel

    if issubclass(cls, BaseModel):
        return KIND_PYDANTIC
    return None


def stored_field_names(cls: type) -> list[str]:
    """Return the names of the fields stored for a user class, in declared order.

    Args:
        cls: A dataclass or pydantic model class.

    Returns:
        The ``init=True`` fields of a dataclass, or the ``model_fields`` of a pydantic
        model. Computed fields and private attributes are not stored.

    Raises:
        TypeError: If ``cls`` is not a dataclass or pydantic model class.
    """
    kind = class_kind(cls)
    if kind == KIND_DATACLASS:
        return [f.name for f in dataclasses.fields(cls) if f.init]
    if kind == KIND_PYDANTIC:
        return list(cls.model_fields)  # pyright: ignore[reportAttributeAccessIssue]  # class_kind guarantees a BaseModel subclass
    raise TypeError(f"{cls!r} is not a dataclass or pydantic model class.")


def type_hints(cls: type) -> dict[str, Any]:
    """Return the resolved type hints of a class.

    Args:
        cls: A class.

    Returns:
        Field name to annotation, with ``Annotated`` extras stripped.

    Raises:
        ValueError: If the hints cannot be resolved.
    """
    try:
        return typing.get_type_hints(cls)
    except Exception as exc:
        raise ValueError(f"Cannot get type hints for {cls!r}: {exc}") from exc


def validate_name(name: str) -> None:
    """Check that ``name`` can be used as the name of a user class.

    Args:
        name: The proposed name.

    Raises:
        TypeError: If ``name`` is not a string.
        ValueError: If ``name`` is empty, contains ``@`` or whitespace, or starts with
            the reserved ``orcapod.`` prefix.
    """
    if not isinstance(name, str):
        raise TypeError(f"A type name must be a string, got {name!r}.")
    if not name:
        raise ValueError("A type name must not be empty.")
    if _NAME_FORBIDDEN.search(name):
        raise ValueError(
            f"Invalid type name {name!r}: a name must not contain '@' or whitespace."
        )
    if name.startswith(RESERVED_PREFIX):
        raise ValueError(
            f"Invalid type name {name!r}: the prefix {RESERVED_PREFIX!r} is reserved "
            f"for Orcapod's own types."
        )


def is_main_class(cls: type) -> bool:
    """Return True if ``cls`` was defined in ``__main__`` (or ``__mp_main__``)."""
    return cls.__module__ in MAIN_MODULES


def default_name(cls: type, *, qualified_name: bool | None = None) -> str:
    """Return the name a class gets without a declared name.

    Args:
        cls: A dataclass or pydantic model class.
        qualified: ``None`` applies the location rule: ``__qualname__`` for a class
            defined in ``__main__`` (or ``__mp_main__``), ``module.qualname`` for a
            module class. ``False`` always gives ``__qualname__``; ``True`` always gives
            ``module.qualname``.

    Returns:
        The name component.
    """
    if qualified_name is None:
        qualified_name = not is_main_class(cls)
    if not qualified_name:
        return cls.__qualname__
    if is_main_class(cls):
        # A spawn child names the main module "__mp_main__"; both are "__main__".
        return f"__main__.{cls.__qualname__}"
    from orcapod.utils.canonical_type_names import canonical_module_name

    module = canonical_module_name(cls) or cls.__module__
    return f"{module}.{cls.__qualname__}"


def resolve_name(cls: type) -> str:
    """Return the name component for ``cls``: its declared name or ``default_name``.

    Args:
        cls: A dataclass or pydantic model class.

    Returns:
        The name component of the class's identity.
    """
    declared = _declared.get(cls)
    if declared is not None:
        return declared
    return default_name(cls)


def declare(cls: type, name: str | None, *, qualified_name: bool | None = None) -> None:
    """Record the name a user class is identified by.

    Registration never modifies the class: the name is kept in a process-wide table
    keyed by the class object. Declaring the same class again with the same name is a
    no-op; any other name raises.

    Args:
        cls: A dataclass or pydantic model class.
        name: The declared name, or ``None`` for ``default_name(cls, qualified_name=...)``.
        qualified_name: Only without ``name``: ``False`` for the class name alone,
            ``True`` for ``module.qualname``, ``None`` for the location rule.

    Raises:
        TypeError: If ``cls`` is not a dataclass or pydantic model class.
        ValueError: If both ``name`` and ``qualified_name`` are given, the class is local,
            the name is invalid, or the class is already registered (declared or used)
            under a different name.
    """
    kind = class_kind(cls)
    if kind is None:
        raise TypeError(
            f"Only dataclasses and pydantic models can be registered as types, "
            f"got {cls!r}."
        )
    if name is not None and qualified_name is not None:
        raise ValueError(
            "Pass either name=... or qualified_name=..., not both: qualified_name only chooses "
            "between the class name and its module path when no name is given."
        )
    reject_local_class(cls)
    if name is not None:
        validate_name(name)
        # One spelling per name: the same name typed in two Unicode forms is one name.
        name = unicodedata.normalize("NFC", name)
    effective = (
        name if name is not None else default_name(cls, qualified_name=qualified_name)
    )
    previous = _built.get(cls) or _declared.get(cls)
    if previous is not None and previous != effective:
        raise ValueError(
            f"Cannot register {cls.__module__}.{cls.__qualname__} as {effective!r}: it "
            f"is already registered as {previous!r} in this process, and a class has "
            f"one name. Declare the name once, before the class is first used (for "
            f"example with the @op.register_type decorator)."
        )
    _declared[cls] = effective
    remember(kind, effective, cls)


def reject_local_class(cls: type) -> None:
    """Raise if ``cls`` is defined inside a function.

    Args:
        cls: A class.

    Raises:
        ValueError: If ``cls.__qualname__`` contains ``<locals>``.
    """
    if "<locals>" in cls.__qualname__:
        raise ValueError(
            f"Cannot register local class {cls!r}: a class defined inside a function "
            f"cannot be found again when stored values are read. Define the class at "
            f"module level."
        )


def built_name(cls: type) -> str | None:
    """Return the name ``cls`` was built under in this process, or ``None``."""
    return _built.get(cls)


def identity_of(cls: type) -> str | None:
    """Return the identity string ``cls`` was built with, or ``None`` if not built."""
    return _identity_of.get(cls)


def built_record(cls: type) -> dict[str, Any] | None:
    """Return the identity record ``cls`` was built with, or ``None`` if not built."""
    return _record_of.get(cls)


def mark_built(
    cls: type, name: str, identity: str, record: dict[str, Any] | None = None
) -> None:
    """Record that ``cls`` was built under ``name`` with ``identity``.

    A class has one name per process.

    Args:
        cls: The class.
        name: Its name component.
        identity: Its identity string.
        record: Its identity record.
    """
    _built[cls] = name
    _identity_of[cls] = identity
    if record is not None:
        _record_of[cls] = record


def equivalent_hints(a: Any, b: Any) -> bool:
    """Return True if two type hints are equal up to user-class identity.

    Two user classes are equivalent when they were built with the same identity string,
    for example a notebook class and its re-definition from a re-run cell. Generic hints
    are compared by origin and arguments; union members in any order.

    Args:
        a: A type hint.
        b: A type hint.

    Returns:
        True if the hints denote the same types.
    """
    import types as _types

    if a == b:
        return True
    if isinstance(a, type) and isinstance(b, type):
        ident = _identity_of.get(a)
        return ident is not None and ident == _identity_of.get(b)
    origin_a, origin_b = typing.get_origin(a), typing.get_origin(b)
    if origin_a is None or origin_b is None:
        return False
    args_a, args_b = typing.get_args(a), typing.get_args(b)
    unions = (typing.Union, _types.UnionType)
    if origin_a in unions and origin_b in unions:
        return all(any(equivalent_hints(x, y) for y in args_b) for x in args_a) and all(
            any(equivalent_hints(x, y) for x in args_a) for y in args_b
        )
    if origin_a != origin_b or len(args_a) != len(args_b):
        return False
    return all(equivalent_hints(x, y) for x, y in zip(args_a, args_b))


# ---------------------------------------------------------------------------
# Known classes per (kind, name)
# ---------------------------------------------------------------------------


def remember(kind: str, name: str, cls: type) -> None:
    """Record ``cls`` as a class seen under ``(kind, name)``.

    Args:
        kind: The class kind.
        name: The name component.
        cls: The class object.
    """
    refs = _known.setdefault((kind, name), [])
    refs[:] = [r for r in refs if r() is not None and r() is not cls]
    refs.append(weakref.ref(cls))


def known_classes(kind: str, name: str) -> list[type]:
    """Return the live classes seen under ``(kind, name)``, most recent first."""
    classes = [r() for r in _known.get((kind, name), [])]
    return [c for c in reversed(classes) if c is not None]


def candidate_classes(kind: str, name: str) -> Iterator[type]:
    """Yield the classes that may be the class stored under ``(kind, name)``.

    In order: classes registered or declared under that name in this process, a class
    reachable from ``__main__`` by that name, and a class importable by that dotted
    name. Each candidate still has to match the stored identity. The dotted name is
    imported only when the caller asks for a candidate beyond the earlier ones, so a
    read that a registered class satisfies imports nothing.

    Args:
        kind: The stored kind.
        name: The stored name component.

    Yields:
        Distinct candidate classes of the right kind.
    """
    seen: list[type] = []

    def fresh(cls: Any) -> bool:
        """Record ``cls`` and return True if it is of the stored kind and not seen yet."""
        if class_kind(cls) != kind or cls in seen:
            return False
        seen.append(cls)
        return True

    for cls in known_classes(kind, name):
        if fresh(cls):
            yield cls
    for module_name in MAIN_MODULES:
        module = sys.modules.get(module_name)
        if module is not None:
            obj = _walk_attributes(module, name.split("."))
            if fresh(obj):
                yield obj
    obj = _import_dotted(name)
    if fresh(obj):
        yield obj


def _walk_attributes(root: Any, parts: list[str]) -> Any:
    """Follow ``parts`` as attributes from ``root``; return ``None`` if one is missing."""
    obj = root
    for part in parts:
        obj = getattr(obj, part, None)
        if obj is None:
            return None
    return obj


def _import_dotted(name: str) -> Any:
    """Import the object a dotted name refers to, or return ``None`` if there is none."""
    from orcapod.logical_types.type_utils import _walk_fqcn

    try:
        return _walk_fqcn(name)
    except ImportError:
        return None


# ---------------------------------------------------------------------------
# Identity record and string
# ---------------------------------------------------------------------------


def identity_record(
    kind: str, name: str, fields: Iterable[tuple[str, str]]
) -> dict[str, Any]:
    """Build the identity record of a user class.

    Args:
        kind: ``"dataclass"`` or ``"pydantic"``.
        name: The name component.
        fields: ``(stored field name, type token)`` pairs in declared order.

    Returns:
        ``{"kind": kind, "name": name, "fields": [[field, token], ...]}``. The fields
        keep the given order, which is part of the identity.
    """
    return {
        "kind": kind,
        "name": name,
        "fields": [[field, token] for field, token in fields],
    }


def canonical_json(obj: Any) -> str:
    """Serialize ``obj`` as canonical JSON (sorted object keys, no whitespace).

    Arrays keep their order, so the ``fields`` of an identity record stay in declared
    order.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def identity_string(record: dict[str, Any]) -> str:
    """Return the identity string of an identity record.

    The format is ``f"{name}@{fingerprint}"``, where the fingerprint is the first
    ``FINGERPRINT_HEX_DIGITS`` hex digits of the SHA-256 of ``canonical_json(record)``.
    This is the only definition of the format.

    Args:
        record: An identity record from ``identity_record``.

    Returns:
        The identity string, e.g. ``"Point@76d961e4..."``.
    """
    digest = hashlib.sha256(canonical_json(record).encode("utf-8")).hexdigest()
    return f"{record['name']}@{digest[:FINGERPRINT_HEX_DIGITS]}"


def describe_fields(fields: Iterable[Iterable[str]]) -> str:
    """Render ``[name, type token]`` pairs in their order, e.g. ``{x: int, y: float}``."""
    return "{" + ", ".join(": ".join(pair) for pair in fields) + "}"


def describe_record(record: dict[str, Any]) -> str:
    """Render an identity record for messages, e.g. ``Point (dataclass) {x: int}``."""
    return f"{record['name']} ({record['kind']}) {describe_fields(record['fields'])}"


# ---------------------------------------------------------------------------
# Type tokens
# ---------------------------------------------------------------------------


def probe_record(
    cls: type, converter: TypeConverterProtocol, _path: tuple[type, ...] = ()
) -> dict[str, Any]:
    """Return the identity record ``cls`` would be built with, without building it.

    Neither ``cls`` nor any user class it refers to is registered or given a name, so
    a class tested as a read candidate and rejected can still be declared under a name
    afterwards. A user class already built in this process keeps its built identity.

    Args:
        cls: A dataclass or pydantic model class.
        converter: The type converter that resolves the logical types of field types
            that are not user classes.

    Returns:
        The identity record.

    Raises:
        TypeError: If ``cls`` is not a user class or a field type has no type token.
        RecursiveTypeError: If the class refers back to itself.
        ValueError: If the class is local or its type hints cannot be resolved.
    """
    kind = class_kind(cls)
    if kind is None:
        raise TypeError(f"{cls!r} is not a dataclass or pydantic model class.")
    if cls in _path:
        raise RecursiveTypeError(
            " -> ".join(c.__qualname__ for c in (*_path[_path.index(cls) :], cls))
        )
    reject_local_class(cls)
    hints = type_hints(cls)
    path = (*_path, cls)
    fields = [
        (name, _type_token(hints.get(name, Any), converter, path))
        for name in stored_field_names(cls)
    ]
    return identity_record(kind, _built.get(cls) or resolve_name(cls), fields)


def type_token(annotation: Any, converter: TypeConverterProtocol) -> str:
    """Return the type token of a field annotation.

    Builtins are written bare (``int``); classes with a logical type use its logical
    type name (``orcapod.path``, or a nested user class's identity string); other
    classes use their canonical name; generics are written ``origin[args]``; unions list
    their members sorted, with ``None`` last.

    Args:
        annotation: A field annotation (``Annotated`` extras already stripped).
        converter: The type converter that resolves logical types.

    Returns:
        The type token.

    Raises:
        TypeError: If the annotation is not a type the token rules cover.
    """
    return _type_token(annotation, converter, None)


def _type_token(
    annotation: Any,
    converter: TypeConverterProtocol,
    probing: tuple[type, ...] | None,
) -> str:
    """Return a type token; with ``probing``, without registering any user class.

    Args:
        annotation: A field annotation.
        converter: The type converter that resolves logical types.
        probing: ``None`` to register the classes found, as ``type_token`` does.
            Otherwise the user classes being probed, outermost first: a user class is
            then described by ``probe_record`` instead of being registered.

    Returns:
        The type token.

    Raises:
        TypeError: If the annotation is not a type the token rules cover.
    """
    import types as _types

    if annotation is None or annotation is type(None):
        return "None"
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Union or origin is _types.UnionType:
        members = [_type_token(a, converter, probing) for a in args]
        non_none = sorted(m for m in members if m != "None")
        return " | ".join(non_none + (["None"] if "None" in members else []))
    if origin is Literal:
        return f"Literal[{', '.join(sorted(repr(a) for a in args))}]"
    if origin is not None:
        inner = ", ".join(_type_token(a, converter, probing) for a in args)
        return f"{_class_token(origin)}[{inner}]"
    if isinstance(annotation, type):
        if probing is not None and class_kind(annotation) is not None:
            built = _identity_of.get(annotation)
            if built is not None:
                return built
            return identity_string(probe_record(annotation, converter, probing))
        _ = converter.register_python_class(annotation)  # raises for unsupported types
        lt = converter.get_logical_type_for_python_type(annotation)
        if lt is not None:
            return lt.logical_type_name
        return _class_token(annotation)
    raise TypeError(f"Cannot build a type token for annotation {annotation!r}.")


def _class_token(cls: Any) -> str:
    """Return the type token of a class: its canonical name, builtins written bare."""
    name = canonical_type_name(cls)
    return name.removeprefix("builtins.")


# ---------------------------------------------------------------------------
# One-time notice for __main__ classes (S3)
# ---------------------------------------------------------------------------


def notice_main_class(cls: type, name: str) -> None:
    """Warn once per name that a ``__main__`` class is identified by its name only.

    Args:
        cls: The class being registered.
        name: The name component it was given.
    """
    if not is_main_class(cls) or cls in _declared or name in _noticed_main_names:
        return
    if name != cls.__qualname__:
        return
    _noticed_main_names.add(name)
    warnings.warn(
        f"{name} is defined in a notebook or script, so Orcapod identifies it by its "
        f"name {name!r} and its fields. Results stored with it are found again by any "
        f"class named {name!r} with the same fields, including one moved into a module "
        f"and registered with @op.register_type(name={name!r}). Changing its fields "
        f"makes a new type and recomputes.",
        UserWarning,
        stacklevel=2,
    )
