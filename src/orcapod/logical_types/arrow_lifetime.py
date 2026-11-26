"""Arrow extension types that live as long as the process (ITL-1422)."""

from __future__ import annotations

import ctypes
import threading
from typing import Any

import pyarrow as pa

# Reentrant: building one type may build another.
_LOCK = threading.RLock()


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


class KeptInstances(type):
    """Metaclass of an extension class whose instances live until the process ends.

    A class has one instance for each set of construction arguments: calling the class
    again with equal arguments gives the instance already made, and ``__new__`` and
    ``__init__`` run once for it. Every instance is kept with ``keep_for_process``.

    The instances are per concrete class, never per family of classes: a subclass is
    another Arrow type, with its own hooks (``__arrow_ext_scalar_class__``,
    ``__arrow_ext_class__``, its own ``__init__``), so it has instances of its own. A
    subclass inherits the metaclass, so its instances are kept too.

    This holds the lifetime rule of ``keep_for_process``: a read builds a column's type
    by calling its class (``__arrow_ext_deserialize__``), and every such call gives a
    kept instance, so no read makes a type that a worker thread could destroy. The
    number of kept instances is the number of distinct classes and arguments used, not
    the number of reads.

    Arguments are compared with their types (``1``, ``1.0`` and ``True`` are three
    arguments), and positional and keyword forms of one argument are told apart. They
    must be hashable: an instance that cannot be found again would be made on every
    read, and is refused instead.
    """

    _kept: dict[tuple[Any, ...], pa.ExtensionType] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """Return the instance of ``cls`` for these arguments, making it once.

        Raises:
            TypeError: If an argument is not hashable.
        """
        key = (
            cls,
            tuple((type(value), value) for value in args),
            tuple((name, type(kwargs[name]), kwargs[name]) for name in sorted(kwargs)),
        )
        try:
            kept = KeptInstances._kept.get(key)
        except TypeError as exc:
            raise TypeError(
                f"{cls.__name__}: every construction argument must be hashable, so "
                "that one instance is shared by every use of the type and lives "
                "until the process ends."
            ) from exc
        if kept is None:
            with _LOCK:
                kept = KeptInstances._kept.get(key)
                if kept is None:
                    kept = keep_for_process(super().__call__(*args, **kwargs))
                    KeptInstances._kept[key] = kept
        return kept
