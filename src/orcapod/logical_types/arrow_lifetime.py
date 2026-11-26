"""Arrow extension types that live as long as the process (ITL-1422)."""

from __future__ import annotations

import ctypes
import threading
from typing import Any

import pyarrow as pa

# Bound when this module is imported: pinning a type later looks up no symbol.
_ADD_REFERENCE = ctypes.pythonapi.Py_IncRef
_REFERENCE_TO = ctypes.py_object

# Held only to read and write the table of kept instances, never while a constructor
# or a serializer runs. Reentrant, so a finalizer the garbage collector runs inside
# cannot block on it.
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
    _ADD_REFERENCE(_REFERENCE_TO(extension_type))
    return extension_type


class KeptInstances(type):
    """Metaclass of an extension class whose instances live until the process ends.

    Calling the class returns a kept instance: the first one stored of that concrete
    class with that Arrow extension name, storage type (with the metadata of every
    nested field) and serialized metadata (``__arrow_ext_serialize__()``). Every
    instance a call returns is kept with ``keep_for_process``, once.

    That key is what Arrow tells types apart by when it reads a column: two instances
    Arrow cannot tell apart on read are one type, and anything Arrow can tell apart is
    two. Construction arguments are not compared at all, so nothing depends on their
    equality, their hashability or the order in which they are first used. An argument
    that only a hook reads, and that the metadata does not hold, makes no type of its
    own: the first instance stored is returned for every such argument. That
    distinction could not survive a write and a read anyway; put it into the metadata
    to keep it.

    The instances are per concrete class, never per family of classes: a subclass is
    another Arrow type, with its own hooks (``__arrow_ext_scalar_class__``,
    ``__arrow_ext_class__``, its own ``__init__``), so it has instances of its own. A
    subclass inherits the metaclass, so its instances are kept too. A class is told
    apart by which object it is (``id``; the table holds the class, so the number is
    never used again): a class's own equality or hash is never consulted.

    This holds the lifetime rule of ``keep_for_process``: a read builds a column's type
    by calling its class (``__arrow_ext_deserialize__``), and every such call returns a
    kept instance, so no read hands Arrow a type that a worker thread could destroy.
    The number of kept instances is the number of distinct classes, names, storage
    types and metadata used, not the number of reads.

    How a call works:

    1. It constructs a candidate and reads its key, with no lock held: a constructor
       or a serializer may build other types, on its own thread or on another one it
       waits for. The key is read from Arrow's own description of the candidate
       (``_arrow_identity``), which holds the metadata Arrow serialized when the type
       was built, so the serializer runs once for each construction.
    2. Under one short lock it looks the key up. When it is absent, the candidate is
       stored and then pinned for the process; every caller reads the table under the
       same lock, so none is handed an instance that is not yet pinned. The stored
       instance is returned.

    A candidate that is not stored was never pinned: it is dropped like any other
    object when the call returns, by the thread that made it. An instance is pinned
    once for its key, whatever ``__new__`` returned and however often it is found
    again. A failure while storing leaves nothing pinned and nothing stored.
    """

    # (id of the concrete class, Arrow identity) -> (the class, its kept instance).
    _kept: dict[tuple[int, bytes], tuple[type, pa.ExtensionType]] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """Return the kept instance of ``cls`` that these arguments describe."""
        # User code runs here, with no lock held.
        candidate = super().__call__(*args, **kwargs)
        key = (id(cls), _arrow_identity(candidate))
        with _LOCK:
            kept = KeptInstances._kept
            entry = kept.get(key)
            if entry is not None:
                return entry[1]
            kept[key] = (cls, candidate)
            try:
                keep_for_process(candidate)
            except BaseException:
                # Never leave an instance in the table that is not pinned.
                del kept[key]
                raise
            return candidate


def _arrow_identity(extension_type: pa.ExtensionType) -> bytes:
    """Everything Arrow tells ``extension_type`` apart by when it reads a column.

    The serialized schema of one field of the type: its extension name, its serialized
    metadata as Arrow holds it, and its storage type with the metadata of every nested
    field, which the equality of two ``pa.DataType`` objects does not compare.
    """
    return pa.schema([pa.field("", extension_type)]).serialize().to_pybytes()
