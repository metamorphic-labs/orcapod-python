"""Arrow extension types that live as long as the process (ITL-1422)."""

from __future__ import annotations

import ctypes
import os
import threading
from typing import Any, cast

import pyarrow as pa

# Bound when this module is imported: pinning a type later looks up no symbol.
_ADD_REFERENCE = ctypes.pythonapi.Py_IncRef
_REFERENCE_TO = ctypes.py_object

# Held only to compare and publish prepared entries. Reentrant so same-thread
# interruptions can also use the table.
# A signal handler that interrupts the main thread while it holds this lock can
# deadlock if it waits for another thread to construct an extension type.
_publication_lock_state = (os.getpid(), threading.RLock())


def _lock_for_process() -> threading.RLock:
    """Select a lock this process owns, even in an early child callback."""
    global _publication_lock_state
    pid = os.getpid()
    if _publication_lock_state[0] != pid:
        replacement = (pid, threading.RLock())
        # Allocation can run GC, and a callback may already have installed a lock.
        if _publication_lock_state[0] != pid:
            _publication_lock_state = replacement
    return _publication_lock_state[1]


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
    instance a call returns is kept through the table for the life of the process.

    That key is what Arrow tells types apart by when it reads a column: two instances
    Arrow cannot tell apart on read are one type, and anything Arrow can tell apart is
    two. Construction arguments are not compared at all, so nothing depends on their
    equality, their hashability or the order in which they are first used. An argument
    that only a hook reads, and that the metadata does not hold, makes no type of its
    own: the first instance stored is returned for every such argument. That
    distinction could not survive a write and a read anyway; put it into the metadata
    to keep it. When a subclass's ``__new__`` returns an already published instance,
    normal Python construction can run ``__init__`` on it again, so the subclass
    should preserve any hook state that callers expect to stay stable.

    The instances are per concrete class, never per family of classes: a subclass is
    another Arrow type, with its own hooks (``__arrow_ext_scalar_class__``,
    ``__arrow_ext_class__``, its own ``__init__``), so it has instances of its own. A
    subclass inherits the metaclass, so its instances are kept too. A class is told
    apart by which object it is (``id``; the table holds the class, so the number is
    never used again): a class's own equality or hash is never consulted.

    A read builds a column's type by calling its class
    (``__arrow_ext_deserialize__``), and every such call returns a kept instance, so
    no read hands Arrow a type that a worker thread could destroy.
    The number of kept instances is the number of distinct classes, names, storage
    types and metadata used, not the number of reads.

    How a call works:

    1. It constructs the candidate, reads its key and prepares its owning reference
       with no lock held. Constructors, serializers and GC callbacks can build other
       types, on the same thread or on another one they wait for. The key comes from
       Arrow's own description, which holds the metadata already serialized during
       construction.
    2. Under one short lock, ``setdefault`` installs the prepared entry if the key is
       absent and returns the stored entry. The table itself is permanently rooted,
       so installing the reference pins the instance immediately. Entries are never
       removed or replaced.

    A losing candidate and its prepared reference are discarded after unlocking.
    If insertion fails, the prepared entry is discarded in the same way.
    """

    # (id of the candidate's class, Arrow identity) -> (class, owning reference).
    _kept: dict[tuple[int, bytes], tuple[type, ctypes.py_object]] = {}
    # This permanent reference keeps every published entry alive through shutdown.
    _ADD_REFERENCE(_REFERENCE_TO(_kept))

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """Return the kept instance matching the constructed candidate."""
        candidate = cast(pa.ExtensionType, super().__call__(*args, **kwargs))
        concrete_class = type(candidate)
        key = (id(concrete_class), _arrow_identity(candidate))
        prepared = (concrete_class, _REFERENCE_TO(candidate))
        kept = KeptInstances._kept
        with _lock_for_process():
            entry = kept.setdefault(key, prepared)
        return cast(pa.ExtensionType, entry[1].value)


def _arrow_identity(extension_type: pa.ExtensionType) -> bytes:
    """Everything Arrow tells ``extension_type`` apart by when it reads a column.

    The serialized schema of one field of the type: its extension name, its serialized
    metadata as Arrow holds it, and its storage type with the metadata of every nested
    field, which the equality of two ``pa.DataType`` objects does not compare.
    """
    return pa.schema([pa.field("", extension_type)]).serialize().to_pybytes()
