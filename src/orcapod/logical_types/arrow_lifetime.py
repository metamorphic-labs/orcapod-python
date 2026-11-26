"""Arrow extension types that live as long as the process (ITL-1422)."""

from __future__ import annotations

import ctypes
import struct
from typing import Any

import pyarrow as pa


def keep_for_process(extension_type: pa.ExtensionType) -> pa.ExtensionType:
    """Make ``extension_type`` live until the process ends, and return it.

    The C++ object behind an extension type written in Python owns Python objects, and
    its destructor takes the GIL. Reading a file gives Arrow's worker threads references
    to the types of its columns, and a worker may drop its reference after the read has
    returned. When that is the last reference and the interpreter is finalizing, CPython
    stops the worker inside the destructor: the process aborts on Linux (Python 3.11 and
    3.12) or hangs at exit. A type that is never destroyed cannot do either.

    The reference added here is never given back, on purpose: one held in a module or a
    class would be given back during finalization, which is when it is needed. Use it for
    an instance shared by every use of the type, never for one made per read.

    Args:
        extension_type: The shared instance of an extension type.

    Returns:
        ``extension_type`` itself.
    """
    ctypes.pythonapi.Py_IncRef(ctypes.py_object(extension_type))
    return extension_type


def _exact(value: Any) -> Any:
    """A hashable stand-in for ``value`` that equals only that exact value.

    Python's equality calls ``0.0`` and ``-0.0``, and ``1``, ``1.0`` and ``True``,
    equal; as construction arguments they are different. So a value is told apart by
    its type and its exact value, at every depth of a tuple or frozenset: a float by
    its bits (the two zeros differ, and so do two NaN payloads, while a NaN equals
    itself), anything else by its own equality among values of its own type.
    """
    kind = type(value)
    if kind is float:
        return (kind, struct.pack("<d", value))
    if kind is complex:
        return (kind, struct.pack("<dd", value.real, value.imag))
    if isinstance(value, tuple):
        return (kind, tuple(_exact(item) for item in value))
    if isinstance(value, frozenset):
        return (kind, frozenset(_exact(item) for item in value))
    return (kind, value)


class KeptInstances(type):
    """Metaclass of an extension class whose instances live until the process ends.

    A class has one instance for each set of construction arguments: calling the class
    again with the same arguments gives the instance already made. Every instance a
    call returns is kept with ``keep_for_process``.

    The instances are per concrete class, never per family of classes: a subclass is
    another Arrow type, with its own hooks (``__arrow_ext_scalar_class__``,
    ``__arrow_ext_class__``, its own ``__init__``), so it has instances of its own. A
    subclass inherits the metaclass, so its instances are kept too.

    This holds the lifetime rule of ``keep_for_process``: a read builds a column's type
    by calling its class (``__arrow_ext_deserialize__``), and every such call returns a
    kept instance, so no read hands Arrow a type that a worker thread could destroy.
    The number of kept instances is the number of distinct classes and arguments used,
    not the number of reads.

    Arguments are the same only when they are exactly the same (``_exact``): by type
    and exact value at every depth, so ``0.0`` and ``-0.0``, and ``1``, ``1.0`` and
    ``True``, also inside a tuple or frozenset, are different arguments with instances
    of their own, and the order in which they are first used decides nothing. An object
    of another type is compared by its own equality. Positional and keyword forms of
    one argument are told apart. Arguments must be hashable: an instance that cannot be
    found again would be made on every read, and is refused instead.

    No lock is held while a constructor runs, so a constructor may build other types,
    on its own thread or on another one it waits for. Two threads that ask for an
    instance not yet made may each construct one: the first stored is kept and
    returned to both, and the other, which no caller ever sees, is discarded by the
    thread that made it.
    """

    _kept: dict[tuple[Any, ...], pa.ExtensionType] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """Return the instance of ``cls`` for these arguments, making it when new.

        Raises:
            TypeError: If an argument is not hashable.
        """
        kept_instances = KeptInstances._kept
        try:
            key = (
                cls,
                tuple(_exact(value) for value in args),
                tuple((name, _exact(kwargs[name])) for name in sorted(kwargs)),
            )
            kept = kept_instances.get(key)
        except TypeError as exc:
            raise TypeError(
                f"{cls.__name__}: every construction argument must be hashable, so "
                "that one instance is shared by every use of the type and lives "
                "until the process ends."
            ) from exc
        if kept is None:
            # Constructed with no lock held: user code runs here.
            made = super().__call__(*args, **kwargs)
            # ``setdefault`` is atomic: every caller gets the first instance stored.
            kept = kept_instances.setdefault(key, made)
            if kept is made:
                keep_for_process(made)
        return kept
