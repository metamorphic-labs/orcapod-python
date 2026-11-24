"""Arrow extension types that live as long as the process (ITL-1422)."""

from __future__ import annotations

import ctypes

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
