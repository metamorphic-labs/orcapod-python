"""Arrow extension types that live as long as the process (ITL-1422)."""

from __future__ import annotations

import ctypes
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


class KeptInstances(type):
    """Metaclass of an extension class whose instances live until the process ends.

    Calling the class returns a kept instance: the first one made of that concrete
    class with that Arrow extension name, storage type and serialized metadata
    (``__arrow_ext_serialize__()``). Every instance a call returns is kept with
    ``keep_for_process``.

    That key is what Arrow tells types apart by when it reads a column: two instances
    Arrow cannot tell apart on read are one type, and anything Arrow can tell apart is
    two. Construction arguments are not compared at all, so nothing depends on their
    equality, their hashability or the order in which they are first used. An argument
    that only a hook reads, and that the metadata does not hold, makes no type of its
    own: the first instance made is returned for every such argument. That distinction
    could not survive a write and a read anyway; put it into the metadata to keep it.

    The instances are per concrete class, never per family of classes: a subclass is
    another Arrow type, with its own hooks (``__arrow_ext_scalar_class__``,
    ``__arrow_ext_class__``, its own ``__init__``), so it has instances of its own. A
    subclass inherits the metaclass, so its instances are kept too.

    This holds the lifetime rule of ``keep_for_process``: a read builds a column's type
    by calling its class (``__arrow_ext_deserialize__``), and every such call returns a
    kept instance, so no read hands Arrow a type that a worker thread could destroy.
    The number of kept instances is the number of distinct classes, names, storage
    types and metadata used, not the number of reads.

    Every call constructs an instance, to learn its key. No lock is held while the
    constructor runs, so a constructor may build other types, on its own thread or on
    another one it waits for. An instance that is not the first of its key is never
    returned: the thread that made it discards it, holding the GIL, before the call
    returns.
    """

    _kept: dict[tuple[Any, ...], pa.ExtensionType] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """Return the kept instance of ``cls`` that these arguments describe."""
        # Constructed with no lock held: user code runs here.
        made = super().__call__(*args, **kwargs)
        key = (
            cls,
            made.extension_name,
            made.storage_type,
            made.__arrow_ext_serialize__(),
        )
        # ``setdefault`` is atomic: every caller gets the first instance stored.
        kept = KeptInstances._kept.setdefault(key, made)
        if kept is made:
            keep_for_process(made)
        return kept
